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

// Package tsdb embeds Prometheus' on-disk TSDB so Glouton can persist
// metrics locally without running a separate process.
package tsdb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/glouton/logger"
	"github.com/bleemeo/glouton/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
)

var (
	errPathRequired  = errors.New("tsdb: path is required")
	errStoreClosed   = errors.New("tsdb: store is closed")
	errManagerClosed = errors.New("tsdb: manager is closed")
)

// Status is the lifecycle state of the local TSDB generation held by a
// Manager. Every consumer (metric writes, queries, /data/store-info,
// PromQL and the diagnostic archive) observes the same state.
type Status int

const (
	// StatusDisabled means local persistence is not wanted (or has been
	// normally disabled): no generation is held.
	StatusDisabled Status = iota
	// StatusHealthy means a generation is open and serves both writes and
	// history queries.
	StatusHealthy
	// StatusRecovering means the last generation failed to open or to
	// commit and the Manager is retrying with bounded backoff. A previous
	// still-usable generation may be retained while a reload candidate
	// fails to open.
	StatusRecovering
	// StatusFailed means the bounded retry budget is exhausted: local
	// history persistence stays declared failed until a new configuration
	// is applied.
	StatusFailed
)

// String implements fmt.Stringer.
func (s Status) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusRecovering:
		return "recovering"
	case StatusFailed:
		return "failed"
	default:
		return "disabled"
	}
}

// Store wraps a prometheus/tsdb.DB so it can be used as a Glouton metric
// sink and as a storage.Queryable for the local API.
type Store struct {
	db        *prom_tsdb.DB
	path      string
	retention time.Duration

	// l guards db against a concurrent Close: readers and writers hold it
	// for reading (prom_tsdb.DB is safe for concurrent use), Close holds it
	// for writing so it waits for the in-flight calls.
	l      sync.RWMutex
	closed bool
}

// Options configures a Store.
type Options struct {
	// Path is the directory where TSDB blocks are stored.
	// It must exist and be writable.
	Path string
	// Retention controls how long data is kept on disk.
	// A zero value falls back to Prometheus' default (15d).
	Retention time.Duration
}

// defaultBackoff is the bounded backoff schedule of the recovery loop:
// the wait before each retry, capped at 30s. The number of entries bounds
// the number of attempts per failure episode; when the schedule is
// exhausted the Manager enters StatusFailed.
var defaultBackoff = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
	30 * time.Second,
}

// Open opens (or creates) a TSDB at opts.Path. Open is expensive (it
// replays the WAL) so callers should keep the returned Store across
// agent reloads.
func Open(opts Options) (*Store, error) {
	if opts.Path == "" {
		return nil, errPathRequired
	}

	// The permissions only apply when we create the directory: an existing
	// directory is left untouched, as its permissions may be a deliberate
	// choice from the administrator.
	if err := os.MkdirAll(opts.Path, 0o750); err != nil {
		return nil, fmt.Errorf("tsdb: create dir: %w", err)
	}

	tsdbOpts := prom_tsdb.DefaultOptions()
	if opts.Retention > 0 {
		tsdbOpts.RetentionDuration = int64(opts.Retention / time.Millisecond)
	}

	db, err := prom_tsdb.Open(opts.Path, newSlogger(), prometheus.NewRegistry(), tsdbOpts, nil)
	if err != nil {
		return nil, fmt.Errorf("tsdb: open %s: %w", opts.Path, err)
	}

	return &Store{db: db, path: opts.Path, retention: effectiveRetention(opts.Retention)}, nil
}

// effectiveRetention resolves a zero (unset) retention to Prometheus'
// default.
func effectiveRetention(retention time.Duration) time.Duration {
	if retention > 0 {
		return retention
	}

	return time.Duration(prom_tsdb.DefaultOptions().RetentionDuration) * time.Millisecond
}

// Path returns the on-disk location of the TSDB.
func (s *Store) Path() string { return s.path }

// Retention returns the configured retention duration.
func (s *Store) Retention() time.Duration { return s.retention }

// OldestPointMs returns the timestamp (in ms since epoch) of the
// oldest point that can still be queried, looking at on-disk blocks
// first and falling back to the head block. Returns 0 if the store is
// empty or closed.
func (s *Store) OldestPointMs() int64 {
	s.l.RLock()
	defer s.l.RUnlock()

	if s.closed {
		return 0
	}

	if blocks := s.db.Blocks(); len(blocks) > 0 {
		return blocks[0].MinTime()
	}

	head := s.db.Head()
	minT := head.MinTime()

	// An empty head returns math.MaxInt64; treat that as "no data".
	if minT == int64(^uint64(0)>>1) {
		return 0
	}

	return minT
}

// PushPoints writes the given points into a single appender and commits.
// Points with NaN values are silently skipped (Prometheus' staleness
// convention is encoded elsewhere; we don't want to commit unmappable
// values here).
//
// An error is returned when an append fails or the commit fails: the
// usual cause is a full or read-only disk. The Manager uses it to isolate
// the failed generation from further disk writes and to schedule
// recovery; it must not be swallowed.
func (s *Store) PushPoints(ctx context.Context, points []types.MetricPoint) error {
	if len(points) == 0 {
		return nil
	}

	s.l.RLock()
	defer s.l.RUnlock()

	if s.closed {
		return errStoreClosed
	}

	app := s.db.Appender(ctx)

	var appendErrs []error

	for _, p := range points {
		if math.IsNaN(p.Value) && !value.IsStaleNaN(p.Value) {
			continue
		}

		lbls := labelsFromMap(p.Labels)

		if _, err := app.Append(0, lbls, p.Time.UnixMilli(), p.Value); err != nil {
			appendErrs = append(appendErrs, fmt.Errorf("append %s: %w", lbls.String(), err))
		}
	}

	err := errors.Join(appendErrs...)

	if commitErr := app.Commit(); commitErr != nil {
		// The commit failed: release the appender resources and surface the
		// failure (along with any append error) so the generation is fenced.
		_ = app.Rollback()

		return errors.Join(fmt.Errorf("commit: %w", commitErr), err)
	}

	return err
}

// Querier returns a storage.Querier for the [mint, maxt] window
// (milliseconds since epoch).
func (s *Store) Querier(mint, maxt int64) (storage.Querier, error) {
	s.l.RLock()
	defer s.l.RUnlock()

	if s.closed {
		return nil, errStoreClosed
	}

	return s.db.Querier(mint, maxt)
}

// DiagnosticArchive writes a short status file for support archives.
func (s *Store) DiagnosticArchive(_ context.Context, archive types.ArchiveWriter) error {
	file, err := archive.Create("tsdb.txt")
	if err != nil {
		return err
	}

	s.l.RLock()
	defer s.l.RUnlock()

	fmt.Fprintf(file, "Path: %s\n", s.path)
	fmt.Fprintf(file, "Closed: %v\n", s.closed)

	if !s.closed {
		head := s.db.Head()
		minTime := time.UnixMilli(head.MinTime()).UTC()
		maxTime := time.UnixMilli(head.MaxTime()).UTC()

		fmt.Fprintf(file, "Head min time: %s\n", minTime.Format(time.RFC3339))
		fmt.Fprintf(file, "Head max time: %s\n", maxTime.Format(time.RFC3339))
		fmt.Fprintf(file, "Head series: %d\n", head.NumSeries())
		fmt.Fprintf(file, "Blocks: %d\n", len(s.db.Blocks()))
	}

	return nil
}

// Close flushes the head block and releases the on-disk lock. After
// Close, Querier and PushPoints become no-ops.
func (s *Store) Close() error {
	s.l.Lock()
	defer s.l.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	return s.db.Close()
}

// managedStore is the internal seam between the Manager and one opened
// TSDB generation. *Store satisfies it; tests inject fakes to simulate
// append/commit failures deterministically.
type managedStore interface {
	Path() string
	Retention() time.Duration
	OldestPointMs() int64
	PushPoints(ctx context.Context, points []types.MetricPoint) error
	Querier(mint, maxt int64) (storage.Querier, error)
	DiagnosticArchive(ctx context.Context, archive types.ArchiveWriter) error
	Close() error
}

// Manager owns the current TSDB generation across agent reloads and
// drives its lifecycle: it is the single writer entry point (it
// implements types.PointPusher) and the single dynamic storage.Queryable
// for the local API and PromQL, so every consumer observes the same
// generation and status.
//
// On an append/commit failure the generation is fenced: disk writes are
// isolated and the Manager retries opening the TSDB with bounded
// backoff; in-memory monitoring, thresholds and MQTT sending keep
// running because the Manager merely stops accepting points for the
// failed generation.
type Manager struct {
	openFn  func(Options) (managedStore, error)
	backoff []time.Duration

	mu        sync.Mutex
	closed    bool
	enabled   bool
	path      string
	retention time.Duration

	current *generation
	status  Status
	lastErr error

	// retryCancel/retryDone track the running recovery loop. They are nil
	// when no loop is scheduled.
	retryCancel context.CancelFunc
	retryDone   chan struct{}

	closeOnce sync.Once
	closeDone chan struct{}
}

// generation is one opened TSDB with its tracked in-flight users
// (writers and queriers). A generation is only closed after it has been
// detached and all its users have exited.
type generation struct {
	store managedStore
	users sync.WaitGroup
}

// trackedQuerier releases the generation user count when the querier is
// closed.
type trackedQuerier struct {
	storage.Querier
	generation *generation
	release    sync.Once
}

func (q *trackedQuerier) Close() error {
	err := q.Querier.Close()
	q.release.Do(q.generation.users.Done)

	return err
}

// NewManager returns a Manager using the real Open and the default
// bounded backoff schedule.
func NewManager() *Manager {
	return newManager(
		func(opts Options) (managedStore, error) {
			return Open(opts)
		},
		defaultBackoff,
	)
}

// newManager returns a Manager with an injected opener and backoff
// schedule, used by tests.
func newManager(openFn func(Options) (managedStore, error), backoff []time.Duration) *Manager {
	return &Manager{
		openFn:    openFn,
		backoff:   backoff,
		status:    StatusDisabled,
		closeDone: make(chan struct{}),
	}
}

// Apply adopts the desired local_store policy: enabled toggles the store,
// path and retention select which generation should be active.
//
// The new policy is only committed once a candidate TSDB, when one is
// needed, has been opened and the generation switch is done. When the
// candidate fails to open, the previous still-usable generation is
// retained (writes and history queries keep working against it) and the
// Manager enters recovery, retrying with bounded backoff; an empty
// undeclared handle is never left behind.
//
// Apply returns the open error so callers can surface it (e.g. as a
// configuration warning); the agent itself keeps running.
func (m *Manager) Apply(enabled bool, want Options) error {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()

		return errManagerClosed
	}

	waitRetry := m.cancelRetryLocked()
	m.enabled = enabled
	m.path = want.Path
	m.retention = want.Retention

	m.mu.Unlock()

	if waitRetry != nil {
		<-waitRetry
	}

	if !enabled {
		m.disable()

		return nil
	}

	m.mu.Lock()
	g := m.current
	m.mu.Unlock()

	if g != nil && g.store.Path() == want.Path && retentionMatches(g.store.Retention(), want.Retention) {
		m.mu.Lock()
		m.status = StatusHealthy
		m.lastErr = nil
		m.mu.Unlock()

		return nil
	}

	// Open the candidate before touching the previous generation.
	store, err := m.openFn(want)
	if err == nil {
		m.swapIn(store)

		return nil
	}

	logger.Printf("Local TSDB unavailable, continuing without on-disk metric persistence: %v", err)

	m.mu.Lock()
	m.lastErr = err
	waitRetry = m.startRetryLocked()
	m.mu.Unlock()

	if waitRetry != nil {
		<-waitRetry
	}

	return err
}

// disable performs the normal disable path: detach the generation, wait
// for its users and close it.
func (m *Manager) disable() {
	m.mu.Lock()
	g := m.current
	m.current = nil
	m.status = StatusDisabled
	m.lastErr = nil
	m.mu.Unlock()

	m.retire(g)
}

// swapIn installs an already opened store as the current generation and
// retires the previous one once its users exit.
func (m *Manager) swapIn(store managedStore) {
	m.mu.Lock()
	old := m.current
	m.current = &generation{store: store}
	m.status = StatusHealthy
	m.lastErr = nil
	m.mu.Unlock()

	m.retire(old)

	logger.V(0).Printf("Local TSDB enabled at %s (retention %s)", store.Path(), store.Retention())
}

// PushPoints implements types.PointPusher. Points are forwarded to the
// current healthy generation only; when the store is disabled, failed
// or recovering they are dropped for local persistence (the in-memory
// pusher upstream is unaffected).
func (m *Manager) PushPoints(ctx context.Context, points []types.MetricPoint) {
	if len(points) == 0 {
		return
	}

	m.mu.Lock()

	g := m.current
	if g != nil {
		g.users.Add(1)
	}

	m.mu.Unlock()

	if g == nil {
		return
	}

	err := g.store.PushPoints(ctx, points)
	g.users.Done()

	if err != nil {
		m.handleWriteFailure(g, err)
	}
}

// handleWriteFailure fences a generation whose append/commit failed: it
// is detached (so no further disk write reaches it), a bounded recovery
// loop is scheduled, and the failed store is closed once its users have
// exited.
func (m *Manager) handleWriteFailure(g *generation, cause error) {
	m.mu.Lock()

	if m.closed || m.current != g {
		m.mu.Unlock()

		return
	}

	m.current = nil
	m.lastErr = cause
	waitRetry := m.startRetryLocked()

	m.mu.Unlock()

	if waitRetry != nil {
		<-waitRetry
	}

	logger.Printf(
		"Local TSDB write failed at %s, disk writes are isolated and recovery is retried: %v",
		g.store.Path(), cause,
	)

	m.retire(g)
}

// Querier implements storage.Queryable. The returned querier always
// targets the current generation, so queries and PromQL share the same
// generation view. When no generation is usable the no-op querier is
// returned: the merged in-memory data still serves recent points while
// the status (reported separately through /data/store-info) marks
// history as unavailable.
func (m *Manager) Querier(mint, maxt int64) (storage.Querier, error) {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()

		return nil, errManagerClosed
	}

	g := m.current
	if g == nil {
		m.mu.Unlock()

		return storage.NoopQuerier(), nil
	}

	g.users.Add(1)

	m.mu.Unlock()

	q, err := g.store.Querier(mint, maxt)
	if err != nil {
		g.users.Done()

		return nil, err
	}

	return &trackedQuerier{Querier: q, generation: g}, nil
}

// retire waits for every in-flight user of a detached generation and
// then closes its store.
func (m *Manager) retire(g *generation) {
	if g == nil {
		return
	}

	g.users.Wait()

	if err := g.store.Close(); err != nil {
		logger.V(1).Printf("local TSDB close at %s: %v", g.store.Path(), err)
	}
}

// cancelRetryLocked cancels the running recovery loop, if any, and
// returns its completion channel. Callers must wait on it after
// releasing the mutex (the loop takes the mutex while exiting).
//
// Must be called with m.mu held.
func (m *Manager) cancelRetryLocked() <-chan struct{} {
	if m.retryCancel == nil {
		return nil
	}

	m.retryCancel()
	done := m.retryDone
	m.retryCancel = nil
	m.retryDone = nil

	return done
}

// startRetryLocked schedules a fresh recovery loop targeting the current
// desired options, cancelling and returning the completion channel of a
// previously running loop (callers wait on it outside the mutex).
//
// Must be called with m.mu held.
func (m *Manager) startRetryLocked() <-chan struct{} {
	wait := m.cancelRetryLocked()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	m.retryCancel = cancel
	m.retryDone = done
	m.status = StatusRecovering

	want := Options{Path: m.path, Retention: m.retention}

	go m.runRetries(ctx, want, done)

	return wait
}

// runRetries is the bounded backoff recovery loop. It opens the TSDB
// until it succeeds, the context is cancelled, or the backoff schedule
// is exhausted.
func (m *Manager) runRetries(ctx context.Context, want Options, done chan struct{}) {
	defer close(done)

	for _, wait := range m.backoff {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if ctx.Err() != nil {
			return
		}

		store, err := m.openFn(want)
		if err != nil {
			m.mu.Lock()

			if m.retryDone == done {
				m.lastErr = err
			}

			m.mu.Unlock()

			continue
		}

		m.mu.Lock()

		// Commit the opened generation only if this loop is still current
		// and the desired options still match; otherwise drop it.
		if m.closed || !m.enabled || m.path != want.Path || m.retention != want.Retention || m.retryDone != done {
			m.mu.Unlock()
			_ = store.Close()

			return
		}

		old := m.current
		m.current = &generation{store: store}
		m.status = StatusHealthy
		m.lastErr = nil
		m.retryCancel = nil
		m.retryDone = nil

		m.mu.Unlock()

		logger.V(0).Printf("Local TSDB recovered at %s (retention %s)", want.Path, store.Retention())

		m.retire(old)

		return
	}

	// The bounded budget is exhausted: declare the persistence failed,
	// unless this loop has already been superseded.
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed && m.retryDone == done {
		m.status = StatusFailed
		m.retryCancel = nil
		m.retryDone = nil

		logger.Printf(
			"Local TSDB recovery attempts exhausted at %s, history persistence is failed: %v",
			want.Path, m.lastErr,
		)
	}
}

// Status returns the current lifecycle status.
func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.status
}

// State returns the current status as text ("healthy", "recovering",
// "failed" or "disabled").
func (m *Manager) State() string {
	return m.Status().String()
}

// Persistent reports whether a generation currently serves persistent
// history. It is true while healthy, and also while a previous
// generation is retained during a recovering reload.
func (m *Manager) Persistent() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.current != nil
}

// DesiredEnabled reports whether local persistence is wanted by the
// last applied configuration.
func (m *Manager) DesiredEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.enabled
}

// Retention returns the retention of the current generation, or the
// configured retention (resolved to the default when unset) while no
// generation is open.
func (m *Manager) Retention() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.current != nil {
		return m.current.store.Retention()
	}

	return effectiveRetention(m.retention)
}

// OldestPointMs returns the oldest queryable point of the current
// generation, or 0 when no generation is open.
func (m *Manager) OldestPointMs() int64 {
	m.mu.Lock()
	g := m.current
	m.mu.Unlock()

	if g == nil {
		return 0
	}

	return g.store.OldestPointMs()
}

// LastError returns the last open/write error as text, or the empty
// string.
func (m *Manager) LastError() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.lastErr == nil {
		return ""
	}

	return m.lastErr.Error()
}

// DiagnosticArchive writes the local TSDB lifecycle status for support
// archives. When a generation is active it also includes the store's
// own tsdb.txt, so both share the same generation view.
func (m *Manager) DiagnosticArchive(ctx context.Context, archive types.ArchiveWriter) error {
	file, err := archive.Create("local-store.txt")
	if err != nil {
		return err
	}

	m.mu.Lock()
	enabled := m.enabled
	path := m.path
	retention := m.retention
	status := m.status
	lastErr := m.lastErr
	g := m.current
	m.mu.Unlock()

	fmt.Fprintf(file, "Desired enabled: %v\n", enabled)
	fmt.Fprintf(file, "Configured path: %s\n", path)
	fmt.Fprintf(file, "Configured retention: %s\n", effectiveRetention(retention))
	fmt.Fprintf(file, "Status: %s\n", status)

	if lastErr != nil {
		fmt.Fprintf(file, "Last error: %v\n", lastErr)
	}

	if g == nil {
		fmt.Fprintln(file, "No active TSDB generation.")

		return nil
	}

	fmt.Fprintf(file, "Active generation path: %s\n", g.store.Path())

	return g.store.DiagnosticArchive(ctx, archive)
}

// Close cancels any running recovery loop, detaches the current
// generation, waits for all its users to exit and closes the TSDB. It is
// idempotent and blocks until the shutdown is complete.
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.mu.Lock()

		m.closed = true

		waitRetry := m.cancelRetryLocked()
		g := m.current
		m.current = nil

		m.mu.Unlock()

		go func() {
			defer close(m.closeDone)

			if waitRetry != nil {
				<-waitRetry
			}

			m.retire(g)
		}()
	})

	<-m.closeDone

	return nil
}

// retentionMatches mirrors the resolution rule from setupLocalTSDB: an
// unset retention (want <= 0) always matches the current generation,
// otherwise the durations must be equal.
func retentionMatches(current, want time.Duration) bool {
	return want <= 0 || current == want
}

// newSlogger routes prometheus/tsdb's slog output through Glouton's
// logger, keeping the level at warn+ so normal operation stays quiet.
func newSlogger() *slog.Logger {
	return slog.New(&gloutonSlogHandler{minLevel: slog.LevelWarn})
}

type gloutonSlogHandler struct {
	minLevel slog.Level
	attrs    []slog.Attr
	group    string
}

func (h *gloutonSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *gloutonSlogHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	b.WriteString("tsdb: ")
	b.WriteString(r.Message)

	for _, a := range h.attrs {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value.Any())
	}

	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value.Any())

		return true
	})

	switch {
	case r.Level >= slog.LevelError:
		logger.V(0).Println(b.String())
	default:
		logger.V(1).Println(b.String())
	}

	return nil
}

func (h *gloutonSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)

	return &gloutonSlogHandler{minLevel: h.minLevel, attrs: merged, group: h.group}
}

func (h *gloutonSlogHandler) WithGroup(name string) slog.Handler {
	return &gloutonSlogHandler{minLevel: h.minLevel, attrs: h.attrs, group: name}
}

// labelsFromMap converts a Glouton MetricPoint label map into a
// sorted Prometheus labels.Labels (Prometheus' Append requires sorted
// labels for correctness).
func labelsFromMap(m map[string]string) labels.Labels {
	if len(m) == 0 {
		return labels.EmptyLabels()
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	b := labels.NewScratchBuilder(len(keys))
	for _, k := range keys {
		b.Add(k, m[k])
	}

	return b.Labels()
}
