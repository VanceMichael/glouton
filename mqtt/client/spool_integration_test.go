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

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/glouton/types"

	paho "github.com/eclipse/paho.mqtt.golang"
)

func testSpoolConfigForReloadState(t *testing.T, enabled bool, maxSize int64) SpoolConfig {
	t.Helper()

	return SpoolConfig{
		Enabled:      enabled,
		Name:         testSpoolName,
		Directory:    t.TempDir(),
		MaxSizeBytes: maxSize,
		MaxAge:       0,
	}
}

// TestReloadStateSpoolAdoption verifies the disable->enable reload transition:
// messages accepted while persistence was off are adopted into the spool
// without duplicates, and re-applying the same config never re-injects them.
func TestReloadStateSpoolAdoption(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rs := NewReloadState()
	t.Cleanup(func() { rs.Close() })

	// Run with persistence disabled: messages are only in the FIFO.
	rs.ApplySpool(testSpoolConfigForReloadState(t, false, 0))

	for range 5 {
		rs.AddPendingMessage(ctx, types.Message{
			Retry:      true,
			Topic:      "v1/data",
			Payload:    []byte("x"),
			EnqueuedAt: time.Now(),
		}, true)
	}

	// Enable at reload: backlog is adopted and becomes durable exactly once.
	enabledCfg := testSpoolConfigForReloadState(t, true, 1<<20)
	rs.ApplySpool(enabledCfg)

	stats, ok := rs.SpoolSnapshot()
	if !ok || !stats.Available {
		t.Fatalf("snapshot after enable = %+v ok=%v", stats, ok)
	}

	if stats.LiveRecords != 5 || stats.QueuedRecords != 5 || stats.UndispatchedRecords != 0 {
		t.Fatalf("adoption stats = %+v, want 5 live all queued", stats)
	}

	// Every FIFO message now carries its durable seq.
	for range 5 {
		msg, open := rs.PendingMessage(ctx)
		if !open {
			t.Fatal("FIFO closed unexpectedly")
		}

		if msg.SpoolSeq == 0 {
			t.Fatal("adopted message has no spool seq")
		}

		// Requeue so subsequent iterations can inspect the next message.
		rs.AddPendingMessage(ctx, msg, true)
	}

	// Apply again (plain reload, still enabled): no double adoption, same backlog.
	rs.ApplySpool(enabledCfg)

	stats, _ = rs.SpoolSnapshot()
	if stats.LiveRecords != 5 || stats.QueuedRecords != 5 {
		t.Fatalf("stats after second apply = %+v, want unchanged 5 live", stats)
	}
}

// TestReloadStateSpoolDisableDrains verifies the enable->disable->enable
// transitions: disabling keeps replaying, empty spool releases files, and
// re-enabling opens a fresh WAL.
func TestReloadStateSpoolDisableDrains(t *testing.T) {
	t.Parallel()

	rs := NewReloadState()
	t.Cleanup(func() { rs.Close() })

	cfg := testSpoolConfigForReloadState(t, true, 1<<20)
	rs.ApplySpool(cfg)

	seq, err := rs.SpoolAppend("v1/data", []byte("x"), time.Now())
	if err != nil {
		t.Fatalf("SpoolAppend: %v", err)
	}

	// Disable with a live record: draining keeps the WAL available for replay,
	// no new persistence is accepted.
	disabledCfg := cfg
	disabledCfg.Enabled = false
	rs.ApplySpool(disabledCfg)

	stats, _ := rs.SpoolSnapshot()
	if !stats.Draining || stats.Enabled || !stats.Available {
		t.Fatalf("draining stats = %+v", stats)
	}

	if _, err := rs.SpoolAppend("v1/data", []byte("y"), time.Now()); !errors.Is(err, errSpoolDisabled) {
		t.Fatalf("append while draining = %v, want errSpoolDisabled", err)
	}

	// Retiring the last record drains and removes the WAL.
	if err := rs.SpoolRetire(seq); err != nil {
		t.Fatalf("SpoolRetire: %v", err)
	}

	stats, _ = rs.SpoolSnapshot()
	if stats.Available || stats.LiveRecords != 0 {
		t.Fatalf("drained stats = %+v", stats)
	}

	walPath := filepath.Join(cfg.Directory, spoolDirectoryName, cfg.Name+spoolFileSuffix)
	if _, err := os.Stat(walPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("drained WAL still present (err=%v)", err)
	}

	// Re-enable: a fresh WAL opens and appends work again.
	rs.ApplySpool(cfg)

	stats, _ = rs.SpoolSnapshot()
	if !stats.Available || stats.Draining {
		t.Fatalf("re-enabled stats = %+v", stats)
	}

	if _, err := rs.SpoolAppend("v1/data", []byte("z"), time.Now()); err != nil {
		t.Fatalf("append after re-enable: %v", err)
	}
}

// TestSpoolRecoveryReplayAndRetire models the process-exit scenarios:
// unacknowledged records replay after restart, retired ones do not, and the
// feeder (Dispatch) is the only replay entry point.
func TestSpoolRecoveryReplayAndRetire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cfg := testSpoolConfigForReloadState(t, true, 1<<20)

	// First process: three messages are persisted; only the first is acknowledged.
	rs1 := NewReloadState()
	rs1.ApplySpool(cfg)

	seq1, err := rs1.SpoolAppend("t", []byte("p1"), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := rs1.SpoolAppend("t", []byte("p2"), time.Now()); err != nil {
		t.Fatal(err)
	}

	if _, err := rs1.SpoolAppend("t", []byte("p3"), time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := rs1.SpoolRetire(seq1); err != nil {
		t.Fatalf("retire: %v", err)
	}

	rs1.Close()

	// Second process: recovered records wait for the feeder; they are not in
	// the FIFO yet.
	rs2 := NewReloadState()
	t.Cleanup(rs2.Close)
	rs2.ApplySpool(cfg)

	stats, _ := rs2.SpoolSnapshot()
	if stats.RecoveredRecords != 2 || stats.UndispatchedRecords != 2 || stats.QueuedRecords != 0 {
		t.Fatalf("recovery stats = %+v, want 2 undispatched", stats)
	}

	if rs2.PendingMessagesCount() != 0 {
		t.Fatalf("FIFO must stay empty until the feeder runs, got %d", rs2.PendingMessagesCount())
	}

	// The feeder replays the two records in seq order, once.
	for _, msg := range rs2.SpoolLease(10) {
		rs2.AddPendingMessage(ctx, msg, true)
	}

	payloads := make([]string, 0, 2)

	for range 2 {
		msg, open := rs2.PendingMessage(ctx)
		if !open {
			t.Fatal("FIFO closed")
		}

		if msg.Token != nil || !msg.Retry || msg.SpoolSeq == 0 {
			t.Fatalf("replayed message = %+v, want tokenless retryable spooled message", msg)
		}

		payloads = append(payloads, string(msg.Payload))
	}

	if payloads[0] != "p2" || payloads[1] != "p3" {
		t.Fatalf("replayed payloads = %v, want [p2 p3]", payloads)
	}

	if more := rs2.SpoolLease(10); len(more) != 0 {
		t.Fatalf("feeder dispatched %d extra messages", len(more))
	}

	// Third process replays everything still unacknowledged.
	rs2.Close()

	rs3 := NewReloadState()
	t.Cleanup(rs3.Close)
	rs3.ApplySpool(cfg)

	stats, _ = rs3.SpoolSnapshot()
	if stats.RecoveredRecords != 2 {
		t.Fatalf("second recovery = %+v, want 2 records (seq1 was retired)", stats)
	}
}

// TestReloadStateLeaseUnlease proves a canceled reload cannot strand leased
// spool records: a failed FIFO insertion rolls the lease back, and once the
// FIFO has room the next lease/enqueue delivers every record exactly once.
func TestReloadStateLeaseUnlease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cfg := testSpoolConfigForReloadState(t, true, 1<<20)

	// Previous process left five durable records.
	previous := NewReloadState()
	previous.ApplySpool(cfg)

	for range 5 {
		if _, err := previous.SpoolAppend("v1/data", []byte("x"), time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	previous.Close()

	rs := NewReloadState()
	t.Cleanup(rs.Close)
	rs.ApplySpool(cfg)

	// Fill the in-memory FIFO, as a long broker outage would.
	for range maxPendingMessages {
		rs.AddPendingMessage(ctx, types.Message{Retry: true, Topic: "memory"}, true)
	}

	if rs.PendingMessagesCount() != maxPendingMessages {
		t.Fatalf("FIFO fill count = %d, want %d", rs.PendingMessagesCount(), maxPendingMessages)
	}

	// First run leases the recovered records but its context is canceled while
	// the FIFO is still full: nothing enters the queue.
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	leased := rs.SpoolLease(10)
	if len(leased) != 5 {
		t.Fatalf("leased %d records, want 5", len(leased))
	}

	for _, msg := range leased {
		if ok := rs.AddPendingMessage(canceledCtx, msg, true); ok {
			t.Fatal("AddPendingMessage unexpectedly succeeded on a canceled context with a full FIFO")
		}
	}

	rs.SpoolUnlease(spoolMessageSeqs(leased)...)

	stats, _ := rs.SpoolSnapshot()
	if stats.UndispatchedRecords != 5 || stats.QueuedRecords != 0 || stats.LiveRecords != 5 {
		t.Fatalf("after unlease stats = %+v, want 5 live undispatched", stats)
	}

	// Drain the memory backlog to simulate the broker catching up.
	for range maxPendingMessages {
		if _, open := rs.PendingMessage(ctx); !open {
			t.Fatal("FIFO closed")
		}
	}

	// Next run leases again and every record is enqueued exactly once.
	leased2 := rs.SpoolLease(10)
	if len(leased2) != 5 {
		t.Fatalf("second lease = %d records, want 5", len(leased2))
	}

	for _, msg := range leased2 {
		if ok := rs.AddPendingMessage(ctx, msg, true); !ok {
			t.Fatal("re-enqueue after drain must succeed")
		}
	}

	if rs.PendingMessagesCount() != 5 {
		t.Fatalf("FIFO count = %d, want 5", rs.PendingMessagesCount())
	}

	stats, _ = rs.SpoolSnapshot()
	if stats.QueuedRecords != 5 || stats.UndispatchedRecords != 0 {
		t.Fatalf("post-drain stats = %+v, want 5 queued / 0 undispatched", stats)
	}
}

// blockingWAL blocks the first Write until released, used to prove
// persist-before-enqueue ordering.
type blockingWAL struct {
	walFile
	writeEntered chan struct{}
	release      chan struct{}
	once         sync.Once
}

func (b *blockingWAL) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.writeEntered) })
	<-b.release

	return b.walFile.Write(p)
}

// TestPersistBeforeEnqueue proves no message reaches the FIFO before its spool
// DATA write completes, and that a disk-full failure still publishes the
// message in memory-only mode instead of dropping it.
func TestPersistBeforeEnqueue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cfg := testSpoolConfigForReloadState(t, true, 1<<20)
	cfg.openFile = func(path string) (walFile, error) {
		f, err := openWALFile(path)
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(path, spoolFileSuffix) {
			return &blockingWAL{
				walFile:      f,
				writeEntered: make(chan struct{}),
				release:      make(chan struct{}),
			}, nil
		}

		return f, nil
	}

	rs := NewReloadState()
	t.Cleanup(rs.Close)
	rs.ApplySpool(cfg)

	blocker, ok := rs.spool.f.(*blockingWAL)
	if !ok {
		t.Fatal("blocking WAL not installed")
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		_, _ = rs.SpoolAppend("t", []byte("x"), time.Now())
	}()

	select {
	case <-blocker.writeEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("spool write never started")
	}

	// While the durable write is in flight, nothing is observable in the FIFO.
	if rs.PendingMessagesCount() != 0 {
		t.Fatal("message entered the FIFO before its spool write completed")
	}

	close(blocker.release)
	<-done

	// Disk-full degradation through publishWrapper: the message must still be
	// queued memory-only, with the event counted as disk full.
	faultCfg := testSpoolConfigForReloadState(t, true, 1<<20)
	faultCfg.openFile = func(path string) (walFile, error) {
		f, err := openWALFile(path)
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(path, spoolFileSuffix) {
			return &faultWAL{walFile: f, writeErr: testDiskFullError()}, nil
		}

		return f, nil
	}

	rsFault := NewReloadState()
	t.Cleanup(rsFault.Close)
	rsFault.ApplySpool(faultCfg)

	c := &Client{
		opts:    Options{ID: "test", ReloadState: rsFault},
		encoder: &encoder{},
	}

	if err := c.publishWrapper(ctx, "t", []byte("payload"), true); err != nil {
		t.Fatalf("publishWrapper on disk full: %v", err)
	}

	msg, open := rsFault.PendingMessage(ctx)
	if !open {
		t.Fatal("degraded message missing from FIFO")
	}

	if msg.SpoolSeq != 0 {
		t.Fatal("disk-full degraded message must not carry a seq")
	}

	if stats, _ := rsFault.SpoolSnapshot(); stats.DiskFullEvents == 0 {
		t.Fatal("disk full event not counted")
	}
}

// TestClientRunReloadSpoolSingleOwner verifies that across two in-process runs
// (a reload) the spool recovery happens exactly once and no record is replayed
// twice: the long-lived ReloadState keeps the spool and the FIFO, while each run
// owns its own short-lived feeder/ack goroutines.
func TestClientRunReloadSpoolSingleOwner(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := SpoolConfig{
		Enabled:      true,
		Name:         "reload",
		Directory:    dir,
		MaxSizeBytes: 1 << 20,
		MaxAge:       0,
	}

	// Simulate a previous process: three durable, unacknowledged records.
	previous := NewReloadState()
	previous.ApplySpool(cfg)

	for range 3 {
		if _, err := previous.SpoolAppend("v1/data", []byte("x"), time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	previous.Close()

	// New process: the ReloadState survives in-process reloads.
	rs := NewReloadState()
	t.Cleanup(rs.Close)

	runOnce := func() {
		c := New(Options{
			ID:          "reload",
			ReloadState: rs,
			OptionsFunc: func(context.Context) (*paho.ClientOptions, error) {
				// An unreachable local broker: TCP refuses immediately, so no
				// network traffic and the retry backoff (>=5s) never accumulates
				// enough attempts during the short test runs.
				return paho.NewClientOptions().AddBroker("tcp://127.0.0.1:1"), nil
			},
			SpoolEnabled: true, SpoolDirectory: dir,
			SpoolMaxSize: 1 << 20,
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			defer close(done)
			c.Run(ctx)
		}()

		// Let the feeder tick at least once so records reach the FIFO.
		time.Sleep(2 * spoolDispatchInterval)
		cancel()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("client Run did not stop after reload cancel")
		}
	}

	runOnce()
	runOnce()

	stats, _ := rs.SpoolSnapshot()

	// Recovery counters come from the single open: exactly 3, never 6.
	if stats.RecoveredRecords != 3 {
		t.Fatalf("recovered records across reload = %d, want 3 (single recovery)", stats.RecoveredRecords)
	}

	// FIFO (possibly dispatched) plus still-undispatched records sum to the 3
	// live records: nothing duplicated, nothing lost.
	inFIFO := rs.PendingMessagesCount()
	total := inFIFO + stats.UndispatchedRecords

	if total != 3 || stats.LiveRecords != 3 {
		t.Fatalf("after reload: live=%d fifo=%d undispatched=%d, want 3 total",
			stats.LiveRecords, inFIFO, stats.UndispatchedRecords)
	}
}

// memArchive is an in-memory types.ArchiveWriter for diagnostics tests.
type memArchive struct {
	files   map[string]*bytes.Buffer
	current string
}

func newMemArchive() *memArchive {
	return &memArchive{files: make(map[string]*bytes.Buffer)}
}

func (a *memArchive) Create(filename string) (io.Writer, error) {
	b := &bytes.Buffer{}
	a.files[filename] = b
	a.current = filename

	return b, nil
}

func (a *memArchive) CurrentFileName() string { return a.current }

func TestSpoolDiagnosticArchive(t *testing.T) {
	t.Parallel()

	rs := NewReloadState()
	t.Cleanup(rs.Close)
	rs.ApplySpool(testSpoolConfigForReloadState(t, true, 1<<20))

	if _, err := rs.SpoolAppend("v1/data", []byte("x"), time.Now()); err != nil {
		t.Fatal(err)
	}

	c := &Client{
		opts:    Options{ID: "Open Source", ReloadState: rs},
		encoder: &encoder{},
	}

	// NaN cannot be JSON-encoded: the failure keeps its existing contract and
	// must also be counted as an encode failure.
	if err := c.PublishAsJSON("v1/data", map[string]any{"v": math.NaN()}, true); err == nil {
		t.Fatal("NaN payload should fail to encode")
	}

	archive := newMemArchive()

	if err := c.DiagnosticArchive(context.Background(), archive); err != nil {
		t.Fatalf("DiagnosticArchive: %v", err)
	}

	buf, ok := archive.files["open-source-mqtt-spool.json"]
	if !ok {
		names := make([]string, 0, len(archive.files))

		for name := range archive.files {
			names = append(names, name)
		}

		t.Fatalf("spool diagnostic file missing, got: %v", names)
	}

	var diag map[string]any
	if err := json.Unmarshal(buf.Bytes(), &diag); err != nil {
		t.Fatalf("invalid spool diagnostic JSON: %v\n%s", err, buf.String())
	}

	for _, key := range []string{
		"enabled", "draining", "locked", "available", "path", "file_bytes",
		"live_records", "queued_records", "undispatched_records", "inflight_records",
		"oldest_record_at", "recovered_records", "evicted_size_records",
		"evicted_size_bytes", "evicted_age_records", "disk_full_events",
		"encode_failed_events", "write_errors", "corrupt_tail_records",
		"corrupt_tail_bytes", "unrecoverable_records", "compactions",
		"last_recovery_at",
	} {
		if _, ok := diag[key]; !ok {
			t.Errorf("diagnostic spool file misses key %q", key)
		}
	}

	if diag["encode_failed_events"].(float64) != 1 {
		t.Errorf("encode_failed_events = %v, want 1", diag["encode_failed_events"])
	}

	if diag["live_records"].(float64) < 1 {
		t.Errorf("live_records = %v, want at least 1", diag["live_records"])
	}
}
