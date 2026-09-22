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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSpoolName = "test"

func testSpoolConfig(t *testing.T, maxSize int64, maxAge time.Duration) SpoolConfig {
	t.Helper()

	return SpoolConfig{
		Enabled:      true,
		Name:         testSpoolName,
		Directory:    t.TempDir(),
		MaxSizeBytes: maxSize,
		MaxAge:       maxAge,
	}
}

func openTestSpool(t *testing.T, cfg SpoolConfig, now func() time.Time) (*mqttSpool, []spoolMessage) {
	t.Helper()

	s, recovered, err := openSpool(context.Background(), cfg, now)
	if err != nil {
		t.Fatalf("openSpool: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s, recovered
}

func reopenTestSpool(t *testing.T, cfg SpoolConfig, now func() time.Time) (*mqttSpool, []spoolMessage) {
	t.Helper()

	s, recovered, err := openSpool(context.Background(), cfg, now)
	if err != nil {
		t.Fatalf("reopen spool: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s, recovered
}

func walPathFor(cfg SpoolConfig) string {
	return filepath.Join(cfg.Directory, spoolDirectoryName, cfg.Name+spoolFileSuffix)
}

func appendN(t *testing.T, s *mqttSpool, n int) []uint64 {
	t.Helper()

	seqs := make([]uint64, n)

	for i := range n {
		seq, err := s.Append(fmt.Sprintf("topic/%d", i), []byte(fmt.Sprintf("payload-%d", i)), time.Now())
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}

		seqs[i] = seq
	}

	return seqs
}

func recoveredSeqs(recovered []spoolMessage) []uint64 {
	seqs := make([]uint64, len(recovered))
	for i, msg := range recovered {
		seqs[i] = msg.seq
	}

	return seqs
}

func TestSpoolAppendRetireReopen(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, recovered := openTestSpool(t, cfg, nil)

	if len(recovered) != 0 {
		t.Fatalf("new spool recovered %d messages, want 0", len(recovered))
	}

	seqs := appendN(t, s, 5)

	if seqs[0] != 1 || seqs[4] != 5 {
		t.Fatalf("seqs = %v, want 1..5", seqs)
	}

	if err := s.Retire(seqs[0]); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, recovered := reopenTestSpool(t, cfg, nil)

	if got := recoveredSeqs(recovered); len(got) != 4 || got[0] != 2 || got[3] != 5 {
		t.Fatalf("recovered seqs = %v, want [2 3 4 5]", got)
	}

	if recovered[0].topic != "topic/1" || string(recovered[0].payload) != "payload-1" {
		t.Fatalf("recovered record mismatch: %+v", recovered[0])
	}

	// Retire every remaining record: a third reopen must replay nothing.
	for _, msg := range recovered {
		if err := s2.Retire(msg.seq); err != nil {
			t.Fatalf("Retire %d: %v", msg.seq, err)
		}
	}

	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, recovered = reopenTestSpool(t, cfg, nil)

	if len(recovered) != 0 {
		t.Fatalf("fully retired spool recovered %d messages, want 0", len(recovered))
	}
}

func TestSpoolRetireIdempotentAndInflight(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)

	seqs := appendN(t, s, 2)

	// First PUBACK retires; duplicate/late PUBACKs are no-op without error.
	if err := s.Retire(seqs[0]); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	if err := s.Retire(seqs[0]); err != nil {
		t.Fatalf("duplicate Retire: %v", err)
	}

	if got := s.BeforeSend(seqs[1]); got != sendOK {
		t.Fatalf("BeforeSend live message = %v, want sendOK", got)
	}

	stats := s.Snapshot()
	if stats.InflightRecords != 1 || stats.LiveRecords != 1 {
		t.Fatalf("stats after send = %+v, want 1 inflight / 1 live", stats)
	}

	if got := s.BeforeSend(seqs[0]); got != sendDropUnknown {
		t.Fatalf("BeforeSend retired seq = %v, want sendDropUnknown", got)
	}
}

func TestSpoolSizeEviction(t *testing.T) {
	t.Parallel()

	// A minimal empty record is header(12) + fixed fields(22) = 34 bytes.
	cfg := testSpoolConfig(t, 100, 0)
	s, _ := openTestSpool(t, cfg, nil)

	seq1, err := s.Append("t", nil, time.Now())
	if err != nil {
		t.Fatalf("Append seq1: %v", err)
	}

	seq2, err := s.Append("t", nil, time.Now())
	if err != nil {
		t.Fatalf("Append seq2: %v", err)
	}

	// 34*3 = 102 > 100: the oldest record must be evicted to make room.
	seq3, err := s.Append("t", nil, time.Now())
	if err != nil {
		t.Fatalf("Append seq3: %v", err)
	}

	if got := s.BeforeSend(seq1); got != sendDropUnknown {
		t.Fatalf("evicted seq BeforeSend = %v, want sendDropUnknown", got)
	}

	wantEvictedBytes := int64(spoolFrameHeaderLen + spoolDataFixedFields + len("t"))

	stats := s.Snapshot()
	if stats.LiveRecords != 2 || stats.EvictedSizeRecords != 1 || stats.EvictedSizeBytes != wantEvictedBytes {
		t.Fatalf("stats after eviction = %+v, want evicted bytes %d", stats, wantEvictedBytes)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Eviction is durable (RETIRE frame): seq1 must not come back after restart.
	_, recovered := reopenTestSpool(t, cfg, nil)

	if got := recoveredSeqs(recovered); len(got) != 2 || got[0] != seq2 || got[1] != seq3 {
		t.Fatalf("recovered seqs = %v, want [%d %d]", got, seq2, seq3)
	}
}

func TestSpoolOversizedRecord(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 10, 0)
	s, _ := openTestSpool(t, cfg, nil)

	_, err := s.Append("t", []byte("this payload is way too big for the limit"), time.Now())
	if !errors.Is(err, ErrSpoolRecordTooLarge) {
		t.Fatalf("Append error = %v, want ErrSpoolRecordTooLarge", err)
	}

	if s.Snapshot().EvictedSizeRecords != 0 {
		t.Fatal("an oversized record must not trigger evictions")
	}
}

func TestSpoolMaxAge(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }

	cfg := testSpoolConfig(t, 1<<20, time.Hour)
	s, _ := openTestSpool(t, cfg, clock)

	oldSeq, err := s.Append("old", nil, now.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("Append old: %v", err)
	}

	youngSeq, err := s.Append("young", nil, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("Append young: %v", err)
	}

	// Expired at recovery: old record dropped and counted, young one kept.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r1, recovered := reopenTestSpool(t, cfg, clock)
	if got := recoveredSeqs(recovered); len(got) != 1 || got[0] != youngSeq {
		t.Fatalf("recovered seqs = %v, want only young %d", got, youngSeq)
	}

	if r1.Snapshot().EvictedAgeRecords != 1 {
		t.Fatalf("EvictedAgeRecords at recovery = %d, want 1", r1.Snapshot().EvictedAgeRecords)
	}

	if err := r1.Close(); err != nil {
		t.Fatalf("Close recovered spool: %v", err)
	}

	// Reopen for the dequeue-time checks.
	s2, _ := openTestSpool(t, cfg, clock)
	t.Cleanup(func() { _ = s2.Close() })

	// Age eviction at dequeue time.
	old2, err := s2.Append("old2", nil, now.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("Append old2: %v", err)
	}

	if got := s2.BeforeSend(old2); got != sendDropExpired {
		t.Fatalf("BeforeSend expired = %v, want sendDropExpired", got)
	}

	if s2.Snapshot().EvictedAgeRecords != 1 {
		t.Fatalf("EvictedAgeRecords on the new instance = %d, want 1 (dequeue)", s2.Snapshot().EvictedAgeRecords)
	}

	// Sanity: the old seq from a previous instance is unknown here.
	if got := s2.BeforeSend(oldSeq); got != sendDropUnknown {
		t.Fatalf("BeforeSend %d = %v, want sendDropUnknown", oldSeq, got)
	}
}

func TestSpoolMaxAgeDisabled(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, clock)

	old, err := s.Append("old", nil, now.Add(-100*24*time.Hour))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := s.BeforeSend(old); got != sendOK {
		t.Fatalf("BeforeSend with maxAge=0 = %v, want sendOK", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, recovered := reopenTestSpool(t, cfg, clock)
	if len(recovered) != 1 {
		t.Fatalf("with maxAge=0 recovered %d messages, want 1", len(recovered))
	}
}

func TestSpoolCorruptTailTruncated(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	seqs := appendN(t, s, 2)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a partial frame at the end.
	f, err := os.OpenFile(walPathFor(cfg), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Write([]byte{spoolMagic0, spoolMagic1, spoolVersion, spoolFrameTypeData, 0xff}); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2, recovered := reopenTestSpool(t, cfg, nil)
	if got := recoveredSeqs(recovered); len(got) != 2 || got[0] != seqs[0] || got[1] != seqs[1] {
		t.Fatalf("recovered seqs = %v, want %v", got, seqs)
	}

	stats := s2.Snapshot()
	// A 5-byte partial header is not a complete record: zero records lost,
	// but the bytes are still reported and truncated.
	if stats.CorruptTailRecords != 0 || stats.CorruptTailBytes < 1 {
		t.Fatalf("partial-header corrupt stats = %+v, want 0 records with bytes dropped", stats)
	}

	// Tail was truncated: file must now contain exactly the two valid frames.
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	_, recovered = reopenTestSpool(t, cfg, nil)
	if len(recovered) != 2 {
		t.Fatalf("after truncation, recovered %d messages, want 2", len(recovered))
	}
}

func TestSpoolCorruptTailBadCRC(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	seqs := appendN(t, s, 2)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := walPathFor(cfg)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the last frame's body; its declared length stays valid,
	// so the CRC mismatch is detected and everything from that frame is dropped.
	data[len(data)-1] ^= 0xff

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, recovered := reopenTestSpool(t, cfg, nil)
	if got := recoveredSeqs(recovered); len(got) != 1 || got[0] != seqs[0] {
		t.Fatalf("recovered seqs = %v, want only %d", got, seqs[0])
	}

	if s2.Snapshot().CorruptTailRecords != 1 {
		t.Fatalf("CorruptTailRecords = %d, want 1", s2.Snapshot().CorruptTailRecords)
	}
}

func TestSpoolUndecodableRecordSkipped(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	seqs := appendN(t, s, 1)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := walPathFor(cfg)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Craft a DATA frame with a CRC-valid but structurally impossible body (3 bytes),
	// followed by a regular valid frame seq=9.
	badBody := []byte{1, 2, 3}
	bad := buildRawFrame(spoolFrameTypeData, badBody)
	good := encodeDataFrame(9, "after", []byte("ok"), time.Now())

	data = append(data, bad...)
	data = append(data, good...)

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, recovered := reopenTestSpool(t, cfg, nil)
	got := recoveredSeqs(recovered)

	if len(got) != 2 || got[0] != seqs[0] || got[1] != 9 {
		t.Fatalf("recovered seqs = %v, want [%d 9]", got, seqs[0])
	}

	if s2.Snapshot().UnrecoverableRecords != 1 {
		t.Fatalf("UnrecoverableRecords = %d, want 1", s2.Snapshot().UnrecoverableRecords)
	}
}

func TestSpoolUnknownVersionQuarantined(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	appendN(t, s, 1)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := walPathFor(cfg)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the magic and version of the very first frame.
	data[0] = 'X'
	data[2] = 99

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s2, recovered := reopenTestSpool(t, cfg, nil)
	if len(recovered) != 0 {
		t.Fatalf("quarantined spool recovered %d messages, want 0", len(recovered))
	}

	stats := s2.Snapshot()
	if stats.QuarantinedFiles != 1 || stats.UnrecoverableRecords < 1 {
		t.Fatalf("quarantine stats = %+v", stats)
	}

	// A fresh empty WAL replaced the quarantined file and accepts new records.
	seq, err := s2.Append("new", nil, time.Now())
	if err != nil {
		t.Fatalf("Append after quarantine: %v", err)
	}

	if seq != 1 {
		t.Fatalf("post-quarantine seq = %d, want 1", seq)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}

	foundQuarantine := false

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), testSpoolName+spoolFileSuffix+".corrupt-") {
			foundQuarantine = true
		}
	}

	if !foundQuarantine {
		t.Fatal("quarantined file not found on disk")
	}
}

func buildRawFrame(frameType byte, body []byte) []byte {
	frame := make([]byte, spoolFrameHeaderLen+len(body))
	frame[0] = spoolMagic0
	frame[1] = spoolMagic1
	frame[2] = spoolVersion
	frame[3] = frameType
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(body)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(body))
	copy(frame[spoolFrameHeaderLen:], body)

	return frame
}

// faultWAL wraps a WAL file and injects write/sync failures.
type faultWAL struct {
	walFile
	writeErr error
	syncErr  error
}

func (f *faultWAL) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}

	return f.walFile.Write(p)
}

func (f *faultWAL) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}

	return f.walFile.Sync()
}

func TestSpoolDiskFullOnAppend(t *testing.T) {
	t.Parallel()

	// The fault is injected through the per-spool opener hook so the test stays
	// race-free alongside parallel tests.
	cfg := testSpoolConfig(t, 1<<20, 0)
	cfg.openFile = func(path string) (walFile, error) {
		f, err := openWALFile(path)
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(path, spoolFileSuffix) {
			return &faultWAL{walFile: f, writeErr: testDiskFullError()}, nil
		}

		return f, nil
	}

	s, recovered, err := openSpool(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("openSpool: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	if len(recovered) != 0 {
		t.Fatalf("new spool recovered %d messages, want 0", len(recovered))
	}

	if _, err := s.Append("t", []byte("x"), time.Now()); !errors.Is(err, ErrSpoolDiskFull) {
		t.Fatalf("Append error = %v, want ErrSpoolDiskFull", err)
	}

	if s.Snapshot().DiskFullEvents == 0 {
		t.Fatal("DiskFullEvents = 0, want at least 1")
	}

	// The failed append fabricated nothing on disk: reopening with a healthy
	// opener replays nothing and writes work again.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	healthyCfg := cfg
	healthyCfg.openFile = nil

	s2, recovered := reopenTestSpool(t, healthyCfg, nil)
	if len(recovered) != 0 {
		t.Fatalf("spool recovered %d messages after failed append, want 0", len(recovered))
	}

	if _, err := s2.Append("t", []byte("y"), time.Now()); err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
}

func TestSpoolCompaction(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	seqs := appendN(t, s, 10)

	for _, seq := range seqs[:5] {
		if err := s.Retire(seq); err != nil {
			t.Fatalf("Retire %d: %v", seq, err)
		}
	}

	stats := s.Snapshot()
	if stats.LiveRecords != 5 || stats.FileBytes == 0 || stats.Compactions == 0 {
		t.Fatalf("stats after partial retire = %+v, want 5 live, compacted file", stats)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, recovered := reopenTestSpool(t, cfg, nil)
	if got := recoveredSeqs(recovered); len(got) != 5 || got[0] != seqs[5] || got[4] != seqs[9] {
		t.Fatalf("recovered seqs after compaction = %v, want 6..10 in order", got)
	}
}

func TestSpoolConcurrent(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<30, 0)
	s, _ := openTestSpool(t, cfg, nil)

	var (
		wg sync.WaitGroup
		l  sync.Mutex
		// failures must be collected from worker goroutines: t.Fatalf is only
		// safe on the test goroutine.
		failures []string
	)

	for worker := range 20 {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for i := range 20 {
				seq, err := s.Append(fmt.Sprintf("topic/%d/%d", worker, i), []byte("x"), time.Now())
				if err != nil {
					l.Lock()
					failures = append(failures, fmt.Sprintf("append w%d i%d: %v", worker, i, err))
					l.Unlock()

					return
				}

				if err := s.Retire(seq); err != nil {
					l.Lock()
					failures = append(failures, fmt.Sprintf("retire w%d i%d: %v", worker, i, err))
					l.Unlock()

					return
				}
			}
		}(worker)
	}

	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("concurrent failures: %s", strings.Join(failures, "; "))
	}

	if stats := s.Snapshot(); stats.LiveRecords != 0 || stats.FileBytes != 0 {
		t.Fatalf("stats after concurrent append/retire = %+v, want empty", stats)
	}
}

func TestSpoolFlockExclusivity(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	appendN(t, s, 1)

	// A second "process" must not be able to open the same spool.
	s2, _, err := openSpool(context.Background(), cfg, nil)
	if !errors.Is(err, errSpoolLocked) {
		if s2 != nil {
			_ = s2.Close()
		}

		t.Fatalf("second open error = %v, want errSpoolLocked", err)
	}

	if s2 == nil || !s2.Snapshot().Locked {
		t.Fatal("locked spool must be exposed with Locked=true for diagnostics")
	}

	// A locked stub must never bypass the flock by reopening on config reload.
	if err := s2.enable(cfg); !errors.Is(err, errSpoolLocked) {
		t.Fatalf("enable on locked spool = %v, want errSpoolLocked", err)
	}

	// The locked handle is not an owner: close it without touching files.
	_ = s2.Close()
}

func TestSpoolFilePermissions(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	appendN(t, s, 1)

	dirInfo, err := os.Stat(filepath.Join(cfg.Directory, spoolDirectoryName))
	if err != nil {
		t.Fatal(err)
	}

	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("spool directory perms = %o, want 700", perm)
	}

	walInfo, err := os.Stat(walPathFor(cfg))
	if err != nil {
		t.Fatal(err)
	}

	if perm := walInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("WAL perms = %o, want 600", perm)
	}

	lockInfo, err := os.Stat(filepath.Join(cfg.Directory, spoolDirectoryName, cfg.Name+spoolLockSuffix))
	if err != nil {
		t.Fatal(err)
	}

	if perm := lockInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("lock file perms = %o, want 600", perm)
	}
}

func TestSpoolDraining(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)
	seqs := appendN(t, s, 1)

	s.disable()

	if stats := s.Snapshot(); !stats.Draining || !stats.Available || stats.Enabled {
		t.Fatalf("draining with backlog stats = %+v, want enabled=false draining=true available=true", stats)
	}

	// New messages are not persisted while draining.
	if _, err := s.Append("new", nil, time.Now()); !errors.Is(err, errSpoolDisabled) {
		t.Fatalf("Append in draining error = %v, want errSpoolDisabled", err)
	}

	// Retiring the last live record drains and removes the files.
	if err := s.Retire(seqs[0]); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	if stats := s.Snapshot(); stats.Available || stats.LiveRecords != 0 {
		t.Fatalf("drained stats = %+v, want unavailable and empty", stats)
	}

	if _, err := os.Stat(walPathFor(cfg)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("drained WAL still present (err=%v)", err)
	}

	// Re-enable: the same owner reopens and a fresh WAL is created (lock kept throughout).
	if err := s.enable(cfg); err != nil {
		t.Fatalf("enable after drain: %v", err)
	}

	if _, err := s.Append("again", nil, time.Now()); err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}

	if stats := s.Snapshot(); !stats.Available || stats.LiveRecords != 1 {
		t.Fatalf("reopened stats = %+v, want available with 1 live", stats)
	}
}

// retireFaultWAL fails writes of RETIRE frames only; DATA frames persist fine.
type retireFaultWAL struct {
	walFile
}

func (f *retireFaultWAL) Write(p []byte) (int, error) {
	if len(p) > spoolFrameHeaderLen && p[3] == spoolFrameRetire {
		return 0, testDiskFullError()
	}

	return f.walFile.Write(p)
}

// TestSpoolAgeEvictionFailureRetries verifies that when the durable eviction
// of an expired record cannot be written, BeforeSend asks for a retry instead
// of dropping (or recycling) the message: nothing is silently lost.
func TestSpoolAgeEvictionFailureRetries(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }

	cfg := testSpoolConfig(t, 1<<20, time.Hour)
	cfg.openFile = func(path string) (walFile, error) {
		f, err := openWALFile(path)
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(path, spoolFileSuffix) {
			return &retireFaultWAL{walFile: f}, nil
		}

		return f, nil
	}

	s, _ := openTestSpool(t, cfg, clock)

	seq, err := s.Append("old", nil, now.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if got := s.BeforeSend(seq); got != sendRetryEviction {
		t.Fatalf("BeforeSend with failing RETIRE = %v, want sendRetryEviction", got)
	}

	stats := s.Snapshot()
	if stats.LiveRecords != 1 {
		t.Fatalf("record must stay live after failed eviction, live=%d", stats.LiveRecords)
	}

	if stats.DiskFullEvents == 0 {
		t.Fatal("failed eviction must be counted as a write/disk-full event")
	}
}

// TestSpoolWritesAfterRecoveryCompaction guards against the compaction reopen
// offset bug: writes following a restart compaction must append at end of file,
// never overwrite the compacted prefix.
func TestSpoolWritesAfterRecoveryCompaction(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)

	// Process 1: three unacknowledged records.
	s1, _ := openTestSpool(t, cfg, nil)
	seqs1 := appendN(t, s1, 3)

	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// Process 2: recovery compacts the non-empty WAL; then DATA and RETIRE
	// frames must append instead of overwriting.
	s2, recovered := reopenTestSpool(t, cfg, nil)
	if len(recovered) != 3 {
		t.Fatalf("process2 recovered %d, want 3", len(recovered))
	}

	seq4, err := s2.Append("t", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	seq5, err := s2.Append("t", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if err := s2.Retire(seqs1[0]); err != nil {
		t.Fatalf("retire after compaction: %v", err)
	}

	seq6, err := s2.Append("t", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// Process 3 must see seq2..seq6: the first record retired, nothing overwritten.
	_, recovered = reopenTestSpool(t, cfg, nil)

	want := []uint64{seqs1[1], seqs1[2], seq4, seq5, seq6}
	if got := recoveredSeqs(recovered); len(got) != len(want) || got[0] != want[0] || got[4] != want[4] {
		t.Fatalf("process3 recovered %v, want %v", got, want)
	}
}

// TestSpoolWritesAfterInProcessCompaction covers the same offset bug through a
// purely in-process compaction (retirement threshold) followed by an append.
func TestSpoolWritesAfterInProcessCompaction(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	s, _ := openTestSpool(t, cfg, nil)

	seqs := appendN(t, s, 6)

	for _, seq := range seqs[:4] {
		if err := s.Retire(seq); err != nil {
			t.Fatal(err)
		}
	}

	// Compaction leaves seq5/seq6 live and reopens the fd at end of file.
	seq7, err := s.Append("t", nil, time.Now())
	if err != nil {
		t.Fatalf("append after in-process compaction: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	_, recovered := reopenTestSpool(t, cfg, nil)

	if got := recoveredSeqs(recovered); len(got) != 3 || got[0] != seqs[4] || got[1] != seqs[5] || got[2] != seq7 {
		t.Fatalf("recovered %v, want [%d %d %d]", got, seqs[4], seqs[5], seq7)
	}
}

// reopenFaultWAL opener fails the second and later opens of the WAL (post-rename
// reopen of a compaction), while temp files and the first open keep working.
type reopenFaultOpener struct {
	mu        sync.Mutex
	walOpens  int
	failAfter int
}

func (o *reopenFaultOpener) open(path string) (walFile, error) {
	f, err := openWALFile(path)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(path, spoolFileSuffix) {
		o.mu.Lock()
		o.walOpens++
		fail := o.walOpens > o.failAfter
		o.mu.Unlock()

		if fail {
			_ = f.Close()

			return nil, errors.New("simulated spool reopen failure")
		}
	}

	return f, nil
}

// TestSpoolCompactionReopenFailureFailsOver verifies that when reopening the
// compacted WAL fails, no further writes are accepted through the stale (now
// unlinked) fd, and re-enabling reopens the compacted file on disk.
func TestSpoolCompactionReopenFailureFailsOver(t *testing.T) {
	t.Parallel()

	opener := &reopenFaultOpener{failAfter: 1}
	cfg := testSpoolConfig(t, 1<<20, 0)
	cfg.openFile = opener.open

	s, _ := openTestSpool(t, cfg, nil)

	// Six records then four retirements trigger a compaction whose post-rename
	// reopen fails: the compacted payload is on disk, but the fd is gone.
	seqs := appendN(t, s, 6)

	for _, seq := range seqs[:4] {
		if err := s.Retire(seq); err != nil {
			t.Fatalf("Retire: %v", err)
		}
	}

	stats := s.Snapshot()
	if stats.Available {
		t.Fatal("spool must be unavailable after a failed compaction reopen")
	}

	if stats.WriteErrors == 0 {
		t.Fatal("compaction reopen failure must be counted as a write error")
	}

	// Appends degrade (memory-only at the client layer); nothing goes to the
	// unlinked inode.
	if _, err := s.Append("t", nil, time.Now()); !errors.Is(err, errSpoolDisabled) {
		t.Fatalf("Append after failover = %v, want errSpoolDisabled", err)
	}

	// Simulate the next run re-enabling once the transient open failure clears:
	// the compacted WAL (live seq5/6) is reopened and new writes append at end.
	opener.failAfter = 1_000

	if err := s.enable(cfg); err != nil {
		t.Fatalf("enable after failover: %v", err)
	}

	stats = s.Snapshot()
	// Compaction threshold is reached after the third retirement (file size is
	// 2x live size): the compacted file and the index both hold seq4..seq6.
	if !stats.Available || stats.LiveRecords != 3 || stats.FileBytes == 0 {
		t.Fatalf("re-enabled stats = %+v, want available with 3 live and non-empty file", stats)
	}

	seq7, err := s.Append("t", nil, time.Now())
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A fresh process sees seq4..seq6 (compacted) and seq7 (appended at end):
	// the failed reopen never silently lost the compacted prefix.
	healthyCfg := cfg
	healthyCfg.openFile = nil

	_, recovered := reopenTestSpool(t, healthyCfg, nil)

	if got := recoveredSeqs(recovered); len(got) != 4 || got[0] != seqs[3] || got[1] != seqs[4] || got[2] != seqs[5] || got[3] != seq7 {
		t.Fatalf("recovered %v, want [%d %d %d %d]", got, seqs[3], seqs[4], seqs[5], seq7)
	}
}

func TestSpoolBadName(t *testing.T) {
	t.Parallel()

	cfg := testSpoolConfig(t, 1<<20, 0)
	cfg.Name = "../escape"

	if _, _, err := openSpool(context.Background(), cfg, nil); !errors.Is(err, errSpoolBadName) {
		t.Fatalf("openSpool error = %v, want errSpoolBadName", err)
	}
}
