// Copyright 2015-2026 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/glouton/types"

	"github.com/prometheus/prometheus/storage"
)

var errDiskFull = errors.New("disk is full (simulated)")

// fakeStore is a controllable managedStore used in place of a real
// on-disk TSDB.
type fakeStore struct {
	mu        sync.Mutex
	path      string
	retention time.Duration
	pushErr   error
	closed    bool
	pushed    int
	oldest    int64
}

func (s *fakeStore) Path() string {
	return s.path
}

func (s *fakeStore) Retention() time.Duration {
	return s.retention
}

func (s *fakeStore) OldestPointMs() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0
	}

	return s.oldest
}

func (s *fakeStore) PushPoints(_ context.Context, points []types.MetricPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errStoreClosed
	}

	if s.pushErr != nil {
		return s.pushErr
	}

	s.pushed += len(points)

	return nil
}

func (s *fakeStore) Querier(mint, maxt int64) (storage.Querier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errStoreClosed
	}

	_ = mint
	_ = maxt

	return storage.NoopQuerier(), nil
}

func (s *fakeStore) DiagnosticArchive(context.Context, types.ArchiveWriter) error {
	return nil
}

func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

func (s *fakeStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func (s *fakeStore) setPushErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pushErr = err
}

func (s *fakeStore) setOldest(ms int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldest = ms
}

func (s *fakeStore) pushedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pushed
}

// openerHarness is the injected opener: the next failures opens return
// failErr, successful opens create and record a fakeStore.
type openerHarness struct {
	mu       sync.Mutex
	failures int
	failErr  error
	opens    []Options
	stores   []*fakeStore
}

func (h *openerHarness) opener(opts Options) (managedStore, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.opens = append(h.opens, opts)

	if h.failures > 0 {
		h.failures--

		return nil, h.failErr
	}

	s := &fakeStore{path: opts.Path, retention: effectiveRetention(opts.Retention)}
	h.stores = append(h.stores, s)

	return s, nil
}

func (h *openerHarness) openCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.opens)
}

func (h *openerHarness) lastOpen() Options {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.opens[len(h.opens)-1]
}

func (h *openerHarness) activeStore() *fakeStore {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.stores[len(h.stores)-1]
}

func (h *openerHarness) setFailures(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.failures = n
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(t *testing.T, timeout, tick time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(tick)
	}

	if !cond() {
		t.Fatalf("timed out waiting for %s", msg)
	}
}

func newTestManager(backoff []time.Duration) (*Manager, *openerHarness) {
	h := &openerHarness{failErr: errDiskFull}
	m := newManager(h.opener, backoff)

	return m, h
}

func testPoints(value float64) []types.MetricPoint {
	return []types.MetricPoint{{
		Point:  types.Point{Time: time.Now(), Value: value},
		Labels: map[string]string{"__name__": "cpu_used"},
	}}
}

func TestManagerNormalLifecycle(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{time.Millisecond})
	t.Cleanup(func() { m.Close() })

	opts := Options{Path: "/var/lib/glouton/tsdb", Retention: 24 * time.Hour}

	if err := m.Apply(true, opts); err != nil {
		t.Fatalf("Apply enabled: %v", err)
	}

	if got := m.Status(); got != StatusHealthy {
		t.Errorf("Status = %v, want healthy", got)
	}

	if !m.Persistent() {
		t.Error("Persistent = false, want true")
	}

	if h.openCount() != 1 {
		t.Errorf("open count = %d, want 1", h.openCount())
	}

	// Same settings: the generation is reused, no extra open.
	if err := m.Apply(true, opts); err != nil {
		t.Fatalf("Apply same: %v", err)
	}

	if h.openCount() != 1 {
		t.Errorf("open count after same Apply = %d, want 1", h.openCount())
	}

	store := h.activeStore()

	m.PushPoints(context.Background(), testPoints(1))

	if got := store.pushedCount(); got != 1 {
		t.Errorf("pushed = %d, want 1", got)
	}

	q, err := m.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}

	_ = q.Close()

	// Normal disable: the generation is closed and points are dropped.
	if err := m.Apply(false, Options{}); err != nil {
		t.Fatalf("Apply disabled: %v", err)
	}

	if got := m.Status(); got != StatusDisabled {
		t.Errorf("Status = %v, want disabled", got)
	}

	if m.Persistent() {
		t.Error("Persistent = true after disable")
	}

	if !store.isClosed() {
		t.Error("store was not closed after disable")
	}

	m.PushPoints(context.Background(), testPoints(2))

	if got := store.pushedCount(); got != 1 {
		t.Errorf("pushed after disable = %d, want 1", got)
	}
}

func TestManagerPathAndRetentionReload(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{time.Millisecond})
	t.Cleanup(func() { m.Close() })

	if err := m.Apply(true, Options{Path: "/a", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply /a: %v", err)
	}

	storeA := h.activeStore()

	// Path change: candidate opens first, old generation closes after.
	if err := m.Apply(true, Options{Path: "/b", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply /b: %v", err)
	}

	if got := h.activeStore().Path(); got != "/b" {
		t.Errorf("active path = %q, want /b", got)
	}

	if !storeA.isClosed() {
		t.Error("generation /a was not closed after path change")
	}

	storeB := h.activeStore()

	// Retention change: same path, new generation.
	if err := m.Apply(true, Options{Path: "/b", Retention: 48 * time.Hour}); err != nil {
		t.Fatalf("Apply retention 48h: %v", err)
	}

	if got := h.activeStore().Retention(); got != 48*time.Hour {
		t.Errorf("retention = %v, want 48h", got)
	}

	if !storeB.isClosed() {
		t.Error("generation 24h was not closed after retention change")
	}

	opensBefore := h.openCount()

	if err := m.Apply(true, Options{Path: "/b", Retention: 48 * time.Hour}); err != nil {
		t.Fatalf("Apply same 48h: %v", err)
	}

	if h.openCount() != opensBefore {
		t.Errorf("open count = %d, want %d (generation should be reused)", h.openCount(), opensBefore)
	}
}

// TestManagerWriteFailureRecovery covers the main scenario: a commit
// failure fences the generation, disk writes are isolated, the bounded
// backoff loop reopens the TSDB and writes resume without a restart;
// history queries see the recovered generation.
func TestManagerWriteFailureRecovery(t *testing.T) {
	t.Parallel()

	// The first backoff is long enough to observe the recovering state
	// before the first retry; the remaining ones are short.
	m, h := newTestManager([]time.Duration{
		50 * time.Millisecond,
		5 * time.Millisecond,
		5 * time.Millisecond,
		5 * time.Millisecond,
	})
	t.Cleanup(func() { m.Close() })

	want := Options{Path: "/tsdb", Retention: 24 * time.Hour}

	if err := m.Apply(true, want); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	failed := h.activeStore()

	m.PushPoints(context.Background(), testPoints(1))

	if got := failed.pushedCount(); got != 1 {
		t.Fatalf("pushed before failure = %d, want 1", got)
	}

	// The next two recovery attempts fail, the third succeeds.
	h.setFailures(2)
	failed.setPushErr(errDiskFull)

	m.PushPoints(context.Background(), testPoints(2))

	// Fenced immediately: recovering, no persistent generation, old store
	// closed, points no longer persisted.
	waitFor(t, 40*time.Millisecond, time.Millisecond, func() bool {
		return m.Status() == StatusRecovering
	}, "recovering state")

	if m.Persistent() {
		t.Error("Persistent = true after write failure")
	}

	if !failed.isClosed() {
		t.Error("failed generation was not closed")
	}

	m.PushPoints(context.Background(), testPoints(3))

	if got := failed.pushedCount(); got != 1 {
		t.Errorf("pushed after failure = %d, want 1", got)
	}

	// After the bounded retries the TSDB recovers.
	waitFor(t, time.Second, 2*time.Millisecond, func() bool {
		return m.Status() == StatusHealthy
	}, "healthy state after recovery")

	if !m.Persistent() {
		t.Error("Persistent = false after recovery")
	}

	recovered := h.activeStore()

	if recovered.Path() != want.Path {
		t.Errorf("recovered path = %q, want %q", recovered.Path(), want.Path)
	}

	if last := h.lastOpen(); last.Retention != want.Retention {
		t.Errorf("reopen retention = %v, want %v", last.Retention, want.Retention)
	}

	// Writes resume against the recovered generation without a restart.
	m.PushPoints(context.Background(), testPoints(4))

	if got := recovered.pushedCount(); got != 1 {
		t.Errorf("recovered pushed = %d, want 1", got)
	}

	// History queries target the recovered generation; only it is open.
	recovered.setOldest(12345)

	if got := m.OldestPointMs(); got != 12345 {
		t.Errorf("OldestPointMs = %d, want 12345", got)
	}

	q, err := m.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Querier after recovery: %v", err)
	}

	_ = q.Close()
}

// TestManagerReloadOpenFailureKeepsOld checks that when a reload
// candidate fails to open, the previous still-usable generation is
// retained: writes and queries keep working while the manager retries,
// and a later successful Apply completes the generation switch.
func TestManagerReloadOpenFailureKeepsOld(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{
		100 * time.Millisecond,
		100 * time.Millisecond,
	})
	t.Cleanup(func() { m.Close() })

	if err := m.Apply(true, Options{Path: "/old", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply /old: %v", err)
	}

	old := h.activeStore()

	// Every candidate (synchronous attempt and recovery loop) fails.
	h.setFailures(10)

	err := m.Apply(true, Options{Path: "/new", Retention: 24 * time.Hour})
	if err == nil {
		t.Fatal("Apply /new succeeded, want open error")
	}

	if got := m.Status(); got != StatusRecovering {
		t.Errorf("Status = %v, want recovering", got)
	}

	// The old generation is retained and still serves.
	if !m.Persistent() {
		t.Error("Persistent = false although old generation is retained")
	}

	if old.isClosed() {
		t.Error("old generation was closed although the candidate failed")
	}

	m.PushPoints(context.Background(), testPoints(1))

	if got := old.pushedCount(); got != 1 {
		t.Errorf("old generation pushed = %d, want 1 (writes must continue)", got)
	}

	q, err := m.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Querier against retained generation: %v", err)
	}

	_ = q.Close()

	// A later successful Apply cancels the failing loop and switches.
	h.setFailures(0)

	if err := m.Apply(true, Options{Path: "/new", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply /new again: %v", err)
	}

	if got := m.Status(); got != StatusHealthy {
		t.Errorf("Status = %v, want healthy", got)
	}

	if got := h.activeStore().Path(); got != "/new" {
		t.Errorf("active path = %q, want /new", got)
	}

	if !old.isClosed() {
		t.Error("old generation was not closed after successful switch")
	}
}

// TestManagerInitialOpenFailureToFailedAndReapply covers a startup open
// failure with no previous generation: recovering, then Failed once the
// bounded budget is exhausted, and healthy again after a new Apply.
func TestManagerInitialOpenFailureToFailedAndReapply(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{
		time.Millisecond,
		time.Millisecond,
		time.Millisecond,
	})
	t.Cleanup(func() { m.Close() })

	h.setFailures(10)

	err := m.Apply(true, Options{Path: "/tsdb", Retention: 24 * time.Hour})
	if err == nil {
		t.Fatal("Apply succeeded, want open error")
	}

	if got := m.Status(); got != StatusRecovering {
		t.Errorf("Status = %v, want recovering", got)
	}

	if m.Persistent() {
		t.Error("Persistent = true although no generation opened")
	}

	// The bounded budget is exhausted: failed state.
	waitFor(t, time.Second, 2*time.Millisecond, func() bool {
		return m.Status() == StatusFailed
	}, "failed state")

	if m.LastError() == "" {
		t.Error("LastError is empty")
	}

	// While failed, queries degrade to the no-op querier, not an error.
	q, err := m.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Querier while failed: %v", err)
	}

	_ = q.Close()

	// Reapplying with a working opener recovers.
	h.setFailures(0)

	if err := m.Apply(true, Options{Path: "/tsdb", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply again: %v", err)
	}

	if got := m.Status(); got != StatusHealthy {
		t.Errorf("Status = %v, want healthy", got)
	}
}

// TestManagerCloseCancelsRetries checks that Close cancels a recovery
// loop scheduled with a long backoff and returns promptly.
func TestManagerCloseCancelsRetries(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{time.Hour})

	if err := m.Apply(true, Options{Path: "/tsdb", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	failed := h.activeStore()
	failed.setPushErr(errDiskFull)

	m.PushPoints(context.Background(), testPoints(1))

	waitFor(t, 100*time.Millisecond, time.Millisecond, func() bool {
		return m.Status() == StatusRecovering
	}, "recovering state")

	start := time.Now()

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Close took %v although the retry loop should be cancelled", d)
	}
}

// TestManagerCloseWaitsForQuerier checks that Close waits for an
// in-flight querier to exit instead of closing the TSDB underneath it.
func TestManagerCloseWaitsForQuerier(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{time.Millisecond})

	if err := m.Apply(true, Options{Path: "/tsdb", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	store := h.activeStore()

	q, err := m.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("Querier: %v", err)
	}

	closed := make(chan struct{})

	go func() {
		_ = m.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while the querier was still open")
	case <-time.After(50 * time.Millisecond):
	}

	if store.isClosed() {
		t.Error("store closed before the querier exited")
	}

	if err := q.Close(); err != nil {
		t.Fatalf("querier Close: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the querier exited")
	}

	if !store.isClosed() {
		t.Error("store was not closed after users exited")
	}

	// Idempotent.
	if err := m.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestManagerConcurrentUseAndShutdown hammers the manager with
// concurrent writers and queriers while generations are fenced and
// recovered several times, then shuts it down. Mostly useful with
// -race.
func TestManagerConcurrentUseAndShutdown(t *testing.T) {
	t.Parallel()

	m, h := newTestManager([]time.Duration{
		time.Millisecond,
		time.Millisecond,
		time.Millisecond,
		time.Millisecond,
	})
	t.Cleanup(func() { m.Close() })

	if err := m.Apply(true, Options{Path: "/tsdb", Retention: 24 * time.Hour}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	stop := make(chan struct{})

	var wg sync.WaitGroup

	for range 4 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				m.PushPoints(context.Background(), testPoints(1))
			}
		}()
	}

	for range 4 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				q, err := m.Querier(0, time.Now().UnixMilli())
				if err == nil {
					_ = q.Close()
				}
			}
		}()
	}

	// Fail and recover several generations while users are active.
	for range 5 {
		failed := h.activeStore()
		failed.setPushErr(errDiskFull)
		h.setFailures(2)

		m.PushPoints(context.Background(), testPoints(1))

		waitFor(t, 2*time.Second, 2*time.Millisecond, func() bool {
			return m.Status() == StatusHealthy && h.activeStore() != failed
		}, "generation recovery")

		h.activeStore().setPushErr(nil)
	}

	close(stop)

	if err := m.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	wg.Wait()

	// Every retired generation must have been closed.
	for _, s := range h.stores {
		if !s.isClosed() {
			t.Errorf("store %q was not closed at shutdown", s.Path())
		}
	}
}
