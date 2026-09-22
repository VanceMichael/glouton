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
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bleemeo/glouton/logger"
	"github.com/bleemeo/glouton/types"

	gofrsflock "github.com/gofrs/flock"
)

const (
	spoolDirectoryName = "mqtt-spool"
	spoolFileSuffix    = ".wal"
	spoolLockSuffix    = ".lock"
	spoolTmpSuffix     = ".tmp"

	spoolMagic0        byte = 'G'
	spoolMagic1        byte = 'S'
	spoolVersion       byte = 1
	spoolFrameTypeData byte = 1
	spoolFrameRetire   byte = 2

	// magic(2) + version(1) + type(1) + body length(4) + crc32(4).
	spoolFrameHeaderLen = 12
	// seq(8) + enqueued at unix nano(8) + topic length(2) + payload length(4).
	spoolDataFixedFields = 22
	// seq(8).
	spoolRetireBodyLen = 8
	// A valid body can't exceed the MQTT payload cap plus its framing fields by much;
	// anything larger while scanning is a corrupt length field.
	spoolMaxBodyLen = types.MaxMQTTPayloadSize + 4096
)

var (
	// ErrSpoolRecordTooLarge is returned when a single record can never fit the
	// configured size limit, even after evicting every other live record.
	ErrSpoolRecordTooLarge = errors.New("mqtt spool: record is larger than the configured size limit")
	// ErrSpoolDiskFull wraps an ENOSPC write failure so callers can expose a
	// dedicated "disk full" diagnostic counter and degrade to the in-memory path.
	ErrSpoolDiskFull = errors.New("mqtt spool: disk is full")

	errSpoolLocked   = errors.New("mqtt spool: locked by another Glouton process")
	errSpoolClosed   = errors.New("mqtt spool: closed")
	errSpoolDisabled = errors.New("mqtt spool: disabled or drained")
	errSpoolBadName  = errors.New("mqtt spool: invalid name")
)

// SpoolConfig configures a persistent pending-message spool.
type SpoolConfig struct {
	// Enabled turns persistence on for retryable messages.
	Enabled bool
	// Name identifies the spool files ("open-source" or "bleemeo").
	Name string
	// Directory is the agent state directory; spool files live in its mqtt-spool subdirectory.
	Directory string
	// MaxSizeBytes caps the live (unacknowledged) size of the spool.
	MaxSizeBytes int64
	// MaxAge is the retention of unacknowledged messages; zero disables age-based eviction.
	MaxAge time.Duration

	// openFile overrides how WAL files are opened. It is used by tests to inject
	// write failures; production code leaves it nil.
	openFile func(path string) (walFile, error)
}

// openFileLocked returns the configured file opener or the production default.
func (cfg SpoolConfig) openFileLocked(path string) (walFile, error) {
	if cfg.openFile != nil {
		return cfg.openFile(path)
	}

	return openWALFile(path)
}

// SpoolStats is the observable state of the spool, exposed in the diagnostic archive.
type SpoolStats struct {
	Enabled              bool      `json:"enabled"`
	Draining             bool      `json:"draining"`
	Locked               bool      `json:"locked"`
	Available            bool      `json:"available"`
	Path                 string    `json:"path"`
	FileBytes            int64     `json:"file_bytes"`
	LiveRecords          int       `json:"live_records"`
	LiveBytes            int64     `json:"live_bytes"`
	QueuedRecords        int       `json:"queued_records"`
	UndispatchedRecords  int       `json:"undispatched_records"`
	InflightRecords      int       `json:"inflight_records"`
	OldestRecordAt       time.Time `json:"oldest_record_at"`
	OldestRecordAge      string    `json:"oldest_record_age"`
	RecoveredRecords     int       `json:"recovered_records"`
	EvictedSizeRecords   int64     `json:"evicted_size_records"`
	EvictedSizeBytes     int64     `json:"evicted_size_bytes"`
	EvictedAgeRecords    int64     `json:"evicted_age_records"`
	DiskFullEvents       int64     `json:"disk_full_events"`
	WriteErrors          int64     `json:"write_errors"`
	CorruptTailRecords   int       `json:"corrupt_tail_records"`
	CorruptTailBytes     int64     `json:"corrupt_tail_bytes"`
	UnrecoverableRecords int       `json:"unrecoverable_records"`
	QuarantinedFiles     int       `json:"quarantined_files"`
	Compactions          int       `json:"compactions"`
	LastRecoveryAt       time.Time `json:"last_recovery_at"`
}

// spoolMessage is a recovered live record, returned in seq order when a spool opens.
type spoolMessage struct {
	seq        uint64
	enqueuedAt time.Time
	topic      string
	payload    []byte
}

// sendDecision tells the ack manager whether a popped message must be sent or skipped.
type sendDecision int

const (
	sendOK sendDecision = iota
	sendDropExpired
	sendDropUnknown
	// sendRetryEviction means an age eviction could not be made durable; the
	// caller must keep the message (and its payload) queued and try again later.
	sendRetryEviction
)

// walFile is the subset of *os.File used by the spool. It is a seam for fault
// injection (disk full, I/O errors) in tests.
type walFile interface {
	io.Reader
	Write(p []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (int64, error)
	Stat() (os.FileInfo, error)
	Close() error
}

// openWALFile opens (creating it if needed) the WAL file with 0600 permissions.
var openWALFile = func(path string) (walFile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // fixed path under the agent state directory
}

// recordState tracks how far an in-memory live record has been dispatched.
// It is not persisted: every record starts undispatched after a process restart.
type recordState int

const (
	stateLive     recordState = iota // on disk, not yet in the in-memory FIFO
	stateQueued                      // in the FIFO, waiting for the ack manager
	stateInflight                    // popped by the ack manager
)

type spoolRecord struct {
	seq        uint64
	enqueuedAt time.Time
	topic      string
	payload    []byte
	frameLen   int
	state      recordState
}

// mqttSpool is an append-only write-ahead queue of MQTT messages.
// A record stays live until a RETIRE frame for its seq is durably written
// (broker PUBACK) or it is explicitly evicted (size or age), which also writes
// a RETIRE frame so the eviction survives a crash.
type mqttSpool struct {
	cfg SpoolConfig
	now func() time.Time

	mu       sync.Mutex
	fl       *gofrsflock.Flock
	f        walFile
	walPath  string
	lockPath string
	closed   bool
	// available is true when the WAL file is open and new DATA frames may be written.
	available bool
	// draining means configuration turned the spool off: existing live records keep
	// being replayed but no new record is persisted; the files are removed once empty.
	draining bool
	// locked records that another process holds the flock; the spool then stays unavailable.
	locked bool

	nextSeq   uint64
	fileBytes int64
	live      map[uint64]*list.Element
	order     *list.List // front is the oldest record; values are *spoolRecord.
	// nextLive is the oldest record not yet dispatched to the FIFO. It only
	// moves forward, keeping Dispatch linear as the backlog drains.
	nextLive *list.Element
	inflight map[uint64]struct{}

	recoveredRecords     int
	evictedSizeRecords   int64
	evictedSizeBytes     int64
	evictedAgeRecords    int64
	diskFullEvents       int64
	writeErrors          int64
	corruptTailRecords   int
	corruptTailBytes     int64
	unrecoverableRecords int
	quarantinedFiles     int
	compactions          int
	lastRecoveryAt       time.Time
}

// spoolNameFromID derives the spool file name from the client ID, reusing the
// same normalization as the diagnostic archive file IDs.
func spoolNameFromID(id string) string {
	name := strings.ToLower(strings.ReplaceAll(id, " ", "-"))

	return name
}

func validateSpoolName(name string) error {
	if name == "" || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("%w: %q", errSpoolBadName, name)
	}

	return nil
}

// openSpool acquires the spool lock, recovers the valid prefix of the WAL and
// returns the live records in seq order. A errSpoolLocked error means another
// process already owns the spool; the caller must degrade to the in-memory path.
func openSpool(ctx context.Context, cfg SpoolConfig, now func() time.Time) (*mqttSpool, []spoolMessage, error) {
	if now == nil {
		now = time.Now
	}

	if err := validateSpoolName(cfg.Name); err != nil {
		return nil, nil, err
	}

	directory := filepath.Join(cfg.Directory, spoolDirectoryName)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, nil, err
	}

	walPath := filepath.Join(directory, cfg.Name+spoolFileSuffix)
	lockPath := filepath.Join(directory, cfg.Name+spoolLockSuffix)

	fl := gofrsflock.New(lockPath)

	acquired, err := fl.TryLock()
	if err != nil {
		return nil, nil, err
	}

	if !acquired {
		locked := &mqttSpool{
			cfg:      cfg,
			now:      now,
			fl:       fl,
			walPath:  walPath,
			lockPath: lockPath,
			locked:   true,
			live:     make(map[uint64]*list.Element),
			order:    list.New(),
			inflight: make(map[uint64]struct{}),
		}

		return locked, nil, errSpoolLocked
	}

	s := &mqttSpool{
		cfg:      cfg,
		now:      now,
		fl:       fl,
		walPath:  walPath,
		lockPath: lockPath,
		live:     make(map[uint64]*list.Element),
		order:    list.New(),
		inflight: make(map[uint64]struct{}),
	}

	f, err := cfg.openFileLocked(walPath)
	if err != nil {
		_ = fl.Unlock()

		return nil, nil, err
	}

	s.f = f
	s.available = true

	quarantined, err := s.recoverLocked(ctx)
	if err != nil {
		_ = s.closeFilesLocked()

		return nil, nil, err
	}

	if quarantined {
		// recoverLocked replaced the WAL with a fresh empty file.
		s.quarantinedFiles++
		s.unrecoverableRecords++

		logger.V(1).Printf("MQTT spool %s: unrecognized WAL quarantined, started a new one", cfg.Name)
	}

	// Reclaim retired frames and any garbage truncated during recovery.
	if _, err := s.compactLocked(); err != nil {
		s.writeErrorsLocked(err, "compaction after recovery")
	}

	recovered := s.liveMessagesLocked()
	s.recoveredRecords = len(recovered)
	s.lastRecoveryAt = now()

	logger.V(1).Printf(
		"MQTT spool %s recovered %d messages (file=%d bytes, corrupt tail records=%d bytes=%d, unrecoverable records=%d)",
		cfg.Name, len(recovered), s.fileBytes, s.corruptTailRecords, s.corruptTailBytes, s.unrecoverableRecords,
	)

	return s, recovered, nil
}

// liveMessagesLocked returns live records ordered from oldest to newest.
func (s *mqttSpool) liveMessagesLocked() []spoolMessage {
	msgs := make([]spoolMessage, 0, s.order.Len())

	for elem := s.order.Front(); elem != nil; elem = elem.Next() {
		rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert
		msgs = append(msgs, spoolMessage{
			seq:        rec.seq,
			enqueuedAt: rec.enqueuedAt,
			topic:      rec.topic,
			payload:    rec.payload,
		})
	}

	return msgs
}

// recoverLocked scans the WAL and rebuilds the live index. It returns true when
// the whole file was unrecognized and quarantined.
func (s *mqttSpool) recoverLocked(ctx context.Context) (bool, error) {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	info, err := s.f.Stat()
	if err != nil {
		return false, err
	}

	fileSize := info.Size()
	if fileSize == 0 {
		s.fileBytes = 0

		return false, nil
	}

	header := make([]byte, spoolFrameHeaderLen)

	var validOffset int64

	// stopWithCorruptTail ends the scan at the beginning of the first bad frame
	// (validOffset). completeFrame reports whether a whole (but invalid) frame
	// was consumed: only such a frame is counted as one lost record, whereas a
	// truncated half-frame counts as zero records but still reports its bytes.
	stopWithCorruptTail := func(alreadyRead int64, completeFrame bool) error {
		tailEnd := validOffset + alreadyRead

		st, statErr := s.f.Stat()
		if statErr == nil && st.Size() > tailEnd {
			tailEnd = st.Size()
		}

		if completeFrame {
			s.corruptTailRecords++
		}

		s.corruptTailBytes += tailEnd - validOffset

		if terr := s.f.Truncate(validOffset); terr != nil {
			s.writeErrorsLocked(terr, "truncate corrupt tail")
		}

		// The scan offset may be past validOffset (partial body read): rewind so
		// subsequent appends write exactly at the end of the valid prefix instead
		// of creating a sparse hole.
		if _, serr := s.f.Seek(validOffset, io.SeekStart); serr != nil {
			s.writeErrorsLocked(serr, "seek after truncate")
		}

		if serr := s.f.Sync(); serr != nil {
			s.writeErrorsLocked(serr, "sync after truncate")
		}

		logger.V(1).Printf(
			"MQTT spool %s: recovered valid prefix of %d bytes, dropped %d corrupt tail bytes",
			s.cfg.Name, validOffset, tailEnd-validOffset,
		)

		return nil
	}

	for {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		n, err := io.ReadFull(s.f, header)
		if errors.Is(err, io.EOF) {
			break // clean frame boundary, nothing left.
		}

		if errors.Is(err, io.ErrUnexpectedEOF) {
			// A truncated header: no complete record is lost.
			return false, stopWithCorruptTail(int64(n), false)
		}

		if err != nil {
			return false, err
		}

		frameStart := validOffset

		// The very first frame identifies the file format. A mismatch means the
		// whole file is unrecognizable: quarantine it and start fresh.
		if frameStart == 0 && (header[0] != spoolMagic0 || header[1] != spoolMagic1 || header[2] != spoolVersion) {
			if qerr := s.quarantineLocked(); qerr != nil {
				return false, qerr
			}

			return true, nil
		}

		if header[0] != spoolMagic0 || header[1] != spoolMagic1 || header[2] != spoolVersion {
			// The whole header was read: one complete-but-invalid frame is lost.
			return false, stopWithCorruptTail(0, true)
		}

		frameType := header[3]
		bodyLen := int64(binary.BigEndian.Uint32(header[4:8]))
		wantCRC := binary.BigEndian.Uint32(header[8:12])

		if bodyLen <= 0 || bodyLen > spoolMaxBodyLen {
			return false, stopWithCorruptTail(0, true)
		}

		body := make([]byte, bodyLen)

		n2, err := io.ReadFull(s.f, body)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// A truncated body: no complete record is lost.
			return false, stopWithCorruptTail(int64(n2), false)
		}

		if err != nil {
			return false, err
		}

		if crc32.ChecksumIEEE(body) != wantCRC {
			return false, stopWithCorruptTail(0, true)
		}

		switch frameType {
		case spoolFrameTypeData:
			rec, derr := decodeDataFrame(body)
			if derr != nil {
				// CRC is valid but the body can't be decoded: the record itself is
				// unrecoverable; scanning continues with the following frames.
				s.unrecoverableRecords++

				logger.V(1).Printf("MQTT spool %s: skipping one unrecoverable record: %v", s.cfg.Name, derr)
			} else {
				s.indexRecordLocked(rec)
			}
		case spoolFrameRetire:
			if bodyLen != spoolRetireBodyLen {
				s.unrecoverableRecords++

				logger.V(1).Printf("MQTT spool %s: skipping malformed RETIRE frame (length=%d)", s.cfg.Name, bodyLen)
			} else {
				seq := binary.BigEndian.Uint64(body)
				s.removeLiveLocked(seq)
			}
		default:
			// Unknown frame type in a version-1 file: can't know its semantics,
			// treat the remainder as a corrupt tail.
			return false, stopWithCorruptTail(0, true)
		}

		validOffset = frameStart + spoolFrameHeaderLen + bodyLen
	}

	// Apply retention while recovering: expired records must not be replayed.
	if s.cfg.MaxAge > 0 {
		cutoff := s.now().Add(-s.cfg.MaxAge)
		expired := 0

		for elem := s.order.Front(); elem != nil; {
			rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert
			next := elem.Next()

			if rec.enqueuedAt.After(cutoff) {
				break // order is chronological by seq.
			}

			s.removeLiveLocked(rec.seq)

			expired++
			elem = next
		}

		if expired > 0 {
			s.evictedAgeRecords += int64(expired)
		}
	}

	// indexRecordLocked maintained nextSeq while scanning; make sure a WAL that
	// contained no usable record still starts at seq 1.
	if s.nextSeq == 0 {
		s.nextSeq = 1
	}

	// Every recovered record starts undispatched; the cursor is chronological.
	s.nextLive = s.order.Front()
	s.fileBytes = validOffset

	return false, nil
}

func (s *mqttSpool) indexRecordLocked(rec *spoolRecord) {
	if rec.seq >= s.nextSeq {
		s.nextSeq = rec.seq + 1
	}

	if _, exists := s.live[rec.seq]; exists {
		// A duplicated DATA frame (shouldn't happen) keeps the first occurrence.
		s.unrecoverableRecords++

		return
	}

	elem := s.order.PushBack(rec)
	s.live[rec.seq] = elem
}

func (s *mqttSpool) removeLiveLocked(seq uint64) {
	elem, ok := s.live[seq]
	if !ok {
		return
	}

	next := elem.Next()
	wasNextLive := s.nextLive == elem

	s.order.Remove(elem)
	delete(s.live, seq)
	delete(s.inflight, seq)

	if wasNextLive {
		s.advanceNextLiveLocked(next)
	}
}

// advanceNextLiveLocked moves the dispatch cursor to the first still-live
// (undispatched) record, starting from from.
func (s *mqttSpool) advanceNextLiveLocked(from *list.Element) {
	for elem := from; elem != nil; elem = elem.Next() {
		if elem.Value.(*spoolRecord).state == stateLive { //nolint:forcetypeassert
			s.nextLive = elem

			return
		}
	}

	s.nextLive = nil
}

func (s *mqttSpool) quarantineLocked() error {
	if err := s.f.Close(); err != nil {
		s.writeErrorsLocked(err, "close WAL before quarantine")
	}

	quarantinePath := fmt.Sprintf("%s.corrupt-%d", s.walPath, s.now().UnixNano())

	if err := os.Rename(s.walPath, quarantinePath); err != nil {
		return fmt.Errorf("quarantine corrupt spool: %w", err)
	}

	f, err := s.cfg.openFileLocked(s.walPath)
	if err != nil {
		return err
	}

	s.f = f
	s.fileBytes = 0
	s.live = make(map[uint64]*list.Element)
	s.order = list.New()
	s.inflight = make(map[uint64]struct{})
	s.nextSeq = 1
	s.nextLive = nil

	return nil
}

// liveBytesLocked returns the total frame size of live records.
func (s *mqttSpool) liveBytesLocked() int64 {
	var total int64

	for _, elem := range s.live {
		total += int64(elem.Value.(*spoolRecord).frameLen) //nolint:forcetypeassert
	}

	return total
}

// Append durably records a new message and returns its seq. The DATA frame (and
// RETIRE frames of size-evicted older messages) are fsynced before Append
// returns, so the record is crash-safe before the caller publishes to MQTT.
func (s *mqttSpool) Append(topic string, payload []byte, enqueuedAt time.Time) (uint64, error) {
	if len(topic) > 0xffff {
		return 0, fmt.Errorf("mqtt spool: topic is too long (%d bytes)", len(topic))
	}

	cost := int64(spoolFrameHeaderLen + spoolDataFixedFields + len(topic) + len(payload))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, errSpoolClosed
	}

	if !s.available || s.draining {
		return 0, errSpoolDisabled
	}

	if cost > s.cfg.MaxSizeBytes {
		return 0, fmt.Errorf("%w: record size %d bytes, limit %d bytes", ErrSpoolRecordTooLarge, cost, s.cfg.MaxSizeBytes)
	}

	// Evict oldest live records until the new frame fits. Evictions are made
	// durable with RETIRE frames so evicted records never come back after a crash.
	var evictedFrames []byte

	for s.liveBytesLocked()+cost > s.cfg.MaxSizeBytes {
		oldest := s.order.Front()
		if oldest == nil {
			break
		}

		rec := oldest.Value.(*spoolRecord) //nolint:forcetypeassert
		s.removeLiveLocked(rec.seq)

		s.evictedSizeRecords++
		s.evictedSizeBytes += int64(rec.frameLen)

		evictedFrames = append(evictedFrames, buildRetireFrame(rec.seq)...)
	}

	if len(evictedFrames) > 0 {
		if err := s.writeAndSyncLocked(evictedFrames); err != nil {
			return 0, err
		}

		s.fileBytes += int64(len(evictedFrames))

		logger.V(1).Printf("MQTT spool %s: size limit reached, evicted %d oldest unacknowledged message(s)",
			s.cfg.Name, s.evictedSizeRecords)
	}

	if s.nextSeq == 0 {
		s.nextSeq = 1
	}

	seq := s.nextSeq
	s.nextSeq++

	frame := encodeDataFrame(seq, topic, payload, enqueuedAt)

	if err := s.writeAndSyncLocked(frame); err != nil {
		return 0, err
	}

	s.fileBytes += cost
	s.live[seq] = s.order.PushBack(&spoolRecord{
		seq:        seq,
		enqueuedAt: enqueuedAt,
		topic:      topic,
		payload:    payload,
		frameLen:   len(frame),
		// A freshly appended record enters the FIFO right away, bypassing Dispatch.
		state: stateQueued,
	})

	s.maybeCompactLocked("after append")

	return seq, nil
}

// encodeDataFrame encodes a DATA frame with the given seq.
func encodeDataFrame(seq uint64, topic string, payload []byte, enqueuedAt time.Time) []byte {
	bodyLen := spoolDataFixedFields + len(topic) + len(payload)
	frame := make([]byte, spoolFrameHeaderLen+bodyLen)

	frame[0] = spoolMagic0
	frame[1] = spoolMagic1
	frame[2] = spoolVersion
	frame[3] = spoolFrameTypeData
	binary.BigEndian.PutUint32(frame[4:8], uint32(bodyLen))

	body := frame[spoolFrameHeaderLen:]
	binary.BigEndian.PutUint64(body[0:8], seq)
	binary.BigEndian.PutUint64(body[8:16], uint64(enqueuedAt.UnixNano()))
	binary.BigEndian.PutUint16(body[16:18], uint16(len(topic)))
	copy(body[18:18+len(topic)], topic)
	binary.BigEndian.PutUint32(body[18+len(topic):22+len(topic)], uint32(len(payload)))
	copy(body[22+len(topic):], payload)
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(body))

	return frame
}

func buildRetireFrame(seq uint64) []byte {
	frame := make([]byte, spoolFrameHeaderLen+spoolRetireBodyLen)

	frame[0] = spoolMagic0
	frame[1] = spoolMagic1
	frame[2] = spoolVersion
	frame[3] = spoolFrameRetire
	binary.BigEndian.PutUint32(frame[4:8], spoolRetireBodyLen)

	body := frame[spoolFrameHeaderLen:]
	binary.BigEndian.PutUint64(body[0:8], seq)
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(body))

	return frame
}

func decodeDataFrame(body []byte) (*spoolRecord, error) {
	if len(body) < spoolDataFixedFields {
		return nil, fmt.Errorf("DATA frame too short: %d bytes", len(body))
	}

	seq := binary.BigEndian.Uint64(body[0:8])
	enqueuedUnixNano := int64(binary.BigEndian.Uint64(body[8:16]))
	topicLen := int(binary.BigEndian.Uint16(body[16:18]))

	if spoolDataFixedFields+topicLen > len(body) {
		return nil, fmt.Errorf("DATA frame truncated in topic (length=%d)", topicLen)
	}

	topic := string(body[18 : 18+topicLen])
	payloadLen := int(binary.BigEndian.Uint32(body[18+topicLen : 22+topicLen]))

	payloadStart := 22 + topicLen
	if payloadStart+payloadLen != len(body) {
		return nil, fmt.Errorf("DATA frame payload length mismatch: header says %d, frame has %d", payloadLen, len(body)-payloadStart)
	}

	payload := body[payloadStart:]

	return &spoolRecord{
		seq:        seq,
		enqueuedAt: time.Unix(0, enqueuedUnixNano),
		topic:      topic,
		payload:    payload,
		frameLen:   spoolFrameHeaderLen + len(body),
	}, nil
}

// Retire durably marks a seq as acknowledged (or otherwise done). A write
// failure leaves the record live, in which case the caller must retry the
// retirement; the message must not be considered acknowledged.
func (s *mqttSpool) Retire(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || !s.available {
		// Files are gone (process exit or drained spool): nothing to retire.
		return nil
	}

	if _, ok := s.live[seq]; !ok {
		// Already retired or evicted: retirement is idempotent.
		return nil
	}

	frame := buildRetireFrame(seq)

	if err := s.writeAndSyncLocked(frame); err != nil {
		return err
	}

	s.fileBytes += int64(len(frame))
	s.removeLiveLocked(seq)

	if s.draining && s.order.Len() == 0 {
		s.detachFilesLocked()

		return nil
	}

	s.maybeCompactLocked("after retire")

	return nil
}

// BeforeSend is called when the ack manager pops a message with the given seq.
// Age eviction is applied here (and made durable) so expired records are never
// (re)sent; size-evicted or unknown seqs are skipped.
func (s *mqttSpool) BeforeSend(seq uint64) sendDecision {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || !s.available {
		return sendDropUnknown
	}

	elem, ok := s.live[seq]
	if !ok {
		return sendDropUnknown
	}

	rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert

	if s.cfg.MaxAge > 0 && s.now().Sub(rec.enqueuedAt) > s.cfg.MaxAge {
		frame := buildRetireFrame(seq)

		if err := s.writeAndSyncLocked(frame); err == nil {
			s.fileBytes += int64(len(frame))
			s.removeLiveLocked(seq)
			s.evictedAgeRecords++

			logger.V(1).Printf("MQTT spool %s: evicted one message older than %s", s.cfg.Name, s.cfg.MaxAge)

			if s.draining && s.order.Len() == 0 {
				s.detachFilesLocked()
			} else {
				s.maybeCompactLocked("after age eviction")
			}

			return sendDropExpired
		} else {
			// The eviction is not durable: the record stays live and still owns
			// its payload. The caller must not send it, must not recycle the
			// payload, and must requeue it so the eviction is retried.
			s.writeErrorsLocked(err, "record age eviction")

			return sendRetryEviction
		}
	}

	s.inflight[seq] = struct{}{}
	rec.state = stateInflight

	return sendOK
}

// Lease returns up to maxRecords oldest undispatched records and marks them
// queued. It is the single replay entry point: recovered records (and records
// adopted when the spool is enabled at reload) reach the ack manager only
// through Lease. A lease is provisional until the caller confirms the records
// entered the FIFO; on failure it must Unlease them so they stay dispatchable.
func (s *mqttSpool) Lease(maxRecords int) []spoolMessage {
	if maxRecords <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || !s.available || s.nextLive == nil {
		return nil
	}

	msgs := make([]spoolMessage, 0, min(maxRecords, s.order.Len()))

	elem := s.nextLive

	for ; elem != nil && len(msgs) < maxRecords; elem = elem.Next() {
		rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert
		if rec.state != stateLive {
			break
		}

		rec.state = stateQueued
		msgs = append(msgs, spoolMessage{
			seq:        rec.seq,
			enqueuedAt: rec.enqueuedAt,
			topic:      rec.topic,
			payload:    rec.payload,
		})
	}

	s.nextLive = elem

	return msgs
}

// Unlease returns leased records to the undispatched set after a failed FIFO
// insertion (canceled reload / closed queue) and rewinds the dispatch cursor,
// so the next run's Lease hands them out again.
func (s *mqttSpool) Unlease(seqs ...uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, seq := range seqs {
		elem, ok := s.live[seq]
		if !ok {
			continue
		}

		rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert
		if rec.state == stateQueued {
			rec.state = stateLive
		}
	}

	// Rewind to the earliest remaining live record: the cursor only moved
	// forward, so the first stateLive from the front is exactly the boundary.
	s.advanceNextLiveLocked(s.order.Front())
}

// disable turns configuration off: new messages are no longer persisted but the
// existing live records keep being replayed until they drain (or the process
// exits). An empty spool releases its WAL immediately.
func (s *mqttSpool) disable() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cfg.Enabled = false
	s.draining = true

	if s.order.Len() == 0 && s.available {
		s.detachFilesLocked()
	}
}

// enable turns configuration back on for an existing spool, re-applying the
// limits and, when a previous drain released the files, reopening a fresh WAL.
func (s *mqttSpool) enable(cfg SpoolConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errSpoolClosed
	}

	// A locked stub never owns the flock: reopening here would bypass the other
	// process. Stay degraded until a real process restart.
	if s.locked {
		return errSpoolLocked
	}

	s.cfg.Enabled = true
	s.draining = false

	if cfg.MaxSizeBytes > 0 {
		s.cfg.MaxSizeBytes = cfg.MaxSizeBytes
	}

	s.cfg.MaxAge = cfg.MaxAge

	if !s.available {
		return s.reopenLocked()
	}

	return nil
}

// IsEnabled reports whether new messages are currently persisted.
func (s *mqttSpool) IsEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.available && !s.draining && !s.closed
}

func (s *mqttSpool) writeAndSyncLocked(p []byte) error {
	n, err := s.f.Write(p)
	if err != nil {
		if isEnospcErr(err) {
			s.diskFullEvents++

			return errors.Join(ErrSpoolDiskFull, err)
		}

		s.writeErrors++

		return fmt.Errorf("mqtt spool write: %w", err)
	}

	if n != len(p) {
		s.writeErrors++

		return fmt.Errorf("mqtt spool write: short write %d/%d", n, len(p))
	}

	if err := s.f.Sync(); err != nil {
		if isEnospcErr(err) {
			s.diskFullEvents++

			return errors.Join(ErrSpoolDiskFull, err)
		}

		s.writeErrors++

		return fmt.Errorf("mqtt spool sync: %w", err)
	}

	return nil
}

func (s *mqttSpool) writeErrorsLocked(err error, operation string) {
	if isEnospcErr(err) {
		s.diskFullEvents++
	} else {
		s.writeErrors++
	}

	logger.V(1).Printf("MQTT spool %s: %s failed: %v", s.cfg.Name, operation, err)
}

// maybeCompactLocked rewrites the WAL with live records only when the file is
// mostly dead space: file size at least twice the live size, or every record is
// dead. Compaction never changes seq values or ordering.
func (s *mqttSpool) maybeCompactLocked(reason string) {
	liveBytes := s.liveBytesLocked()
	if s.fileBytes == 0 {
		return
	}

	if s.order.Len() > 0 && s.fileBytes < 2*liveBytes {
		return
	}

	didWork, err := s.compactLocked()
	if err != nil {
		s.writeErrorsLocked(err, "compaction "+reason)

		return
	}

	if didWork {
		logger.V(2).Printf("MQTT spool %s: compacted WAL to %d live bytes (%s)", s.cfg.Name, liveBytes, reason)
	}
}

// compactLocked rewrites live DATA frames (in seq order) into a temp file and
// atomically replaces the WAL. It reports whether a rewrite happened.
func (s *mqttSpool) compactLocked() (bool, error) {
	liveBytes := s.liveBytesLocked()
	if s.order.Len() == 0 && s.fileBytes == 0 {
		return false, nil
	}

	tmpPath := s.walPath + spoolTmpSuffix

	// Remove a temp file possibly left by a crashed compaction; OpenFile never
	// truncates, so without this its trailing bytes could survive the rewrite.
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	tmp, err := s.cfg.openFileLocked(tmpPath)
	if err != nil {
		return false, err
	}

	var wrote int64

	for elem := s.order.Front(); elem != nil; elem = elem.Next() {
		rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert

		frame := encodeDataFrame(rec.seq, rec.topic, rec.payload, rec.enqueuedAt)

		n, werr := tmp.Write(frame)
		if werr != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)

			return false, werr
		}

		wrote += int64(n)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)

		return false, err
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)

		return false, err
	}

	if err := os.Rename(tmpPath, s.walPath); err != nil {
		_ = os.Remove(tmpPath)

		return false, err
	}

	syncDirectory(filepath.Dir(s.walPath))

	// Reopen through the configured opener so tests observe the replaced file.
	// The new fd starts at offset 0: seek to end so the next Append/Retire does
	// not overwrite the freshly compacted prefix.
	f, err := s.cfg.openFileLocked(s.walPath)
	if err != nil {
		// The rename already replaced the path: the previous fd now references
		// the unlinked inode. Refuse further writes through it; the in-memory
		// index still matches the compacted file on disk, and a later enable()
		// reopens it. Callers meanwhile degrade to the memory-only path.
		s.failoverAfterLostWALLocked()

		return false, err
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		s.failoverAfterLostWALLocked()

		return false, err
	}

	if s.f != nil {
		_ = s.f.Close()
	}

	s.f = f
	s.fileBytes = wrote

	if wrote != liveBytes {
		// frameLen is derived from the same encoding, so this should be impossible.
		return true, fmt.Errorf("mqtt spool: compacted size mismatch %d != %d", wrote, liveBytes)
	}

	s.compactions++

	return wrote > 0 || s.order.Len() == 0, nil
}

// detachFilesLocked closes and removes the WAL once it has no live record left
// (drained spool). The flock is retained so a later reopen stays single-owner.
func (s *mqttSpool) detachFilesLocked() {
	if s.f != nil {
		_ = s.f.Close()
	}

	if err := os.Remove(s.walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.writeErrorsLocked(err, "remove drained WAL")
	}

	s.f = nil
	s.available = false
	s.fileBytes = 0
}

// failoverAfterLostWALLocked drops an fd whose path was just replaced (a
// post-compaction reopen failure). Writes are refused until enable() reopens
// the file, so data can never be silently appended to an unlinked inode. The
// in-memory live index is untouched and matches the compacted file on disk.
func (s *mqttSpool) failoverAfterLostWALLocked() {
	if s.f != nil {
		_ = s.f.Close()
	}

	s.f = nil
	s.available = false
}

// reopenLocked opens (or reopens) the WAL for writes. It is used both after a
// drain (no file expected) and when re-enabling a spool that failed over from a
// lost WAL (the compacted file exists and matches the in-memory live index).
func (s *mqttSpool) reopenLocked() error {
	f, err := s.cfg.openFileLocked(s.walPath)
	if err != nil {
		return err
	}

	// Always position writes at end even if a stale file happens to exist.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()

		return err
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()

		return err
	}

	s.f = f
	s.available = true
	s.draining = false

	// After a drain the live set is empty and the file is absent. When
	// recovering from a failed compaction reopen, the compacted file on disk
	// contains exactly the live records kept in the index.
	if s.order.Len() == 0 {
		s.fileBytes = 0
	} else {
		s.fileBytes = info.Size()
	}

	return nil
}

// Close releases the WAL and the flock. Files are kept for process restart.
func (s *mqttSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeFilesLocked()
}

func (s *mqttSpool) closeFilesLocked() error {
	if s.closed {
		return nil
	}

	s.closed = true

	var err error

	if s.f != nil {
		err = s.f.Close()
	}

	s.available = false

	if s.fl != nil {
		if uerr := s.fl.Unlock(); uerr != nil && err == nil {
			err = uerr
		}
	}

	return err
}

// Snapshot returns a point-in-time view of the spool state for diagnostics.
func (s *mqttSpool) Snapshot() SpoolStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		oldest       time.Time
		queued       int
		undispatched int
	)

	// Count states: cheap at the expected queue depths; bounded by the size cap.
	for elem := s.order.Front(); elem != nil; elem = elem.Next() {
		rec := elem.Value.(*spoolRecord) //nolint:forcetypeassert
		if oldest.IsZero() {
			oldest = rec.enqueuedAt
		}

		switch rec.state {
		case stateQueued:
			queued++
		case stateLive:
			undispatched++
		}
	}

	stats := SpoolStats{
		Enabled:              s.cfg.Enabled,
		Draining:             s.draining,
		Locked:               s.locked,
		Available:            s.available,
		Path:                 s.walPath,
		FileBytes:            s.fileBytes,
		LiveRecords:          s.order.Len(),
		LiveBytes:            s.liveBytesLocked(),
		QueuedRecords:        queued,
		UndispatchedRecords:  undispatched,
		InflightRecords:      len(s.inflight),
		OldestRecordAt:       oldest,
		RecoveredRecords:     s.recoveredRecords,
		EvictedSizeRecords:   s.evictedSizeRecords,
		EvictedSizeBytes:     s.evictedSizeBytes,
		EvictedAgeRecords:    s.evictedAgeRecords,
		DiskFullEvents:       s.diskFullEvents,
		WriteErrors:          s.writeErrors,
		CorruptTailRecords:   s.corruptTailRecords,
		CorruptTailBytes:     s.corruptTailBytes,
		UnrecoverableRecords: s.unrecoverableRecords,
		QuarantinedFiles:     s.quarantinedFiles,
		Compactions:          s.compactions,
		LastRecoveryAt:       s.lastRecoveryAt,
	}

	if !oldest.IsZero() {
		stats.OldestRecordAge = s.now().Sub(oldest).String()
	}

	return stats
}

// syncDirectory fsyncs a directory so a rename is durable. Best effort: on
// platforms or filesystems where it is unsupported the rename is already atomic.
func syncDirectory(directory string) {
	dir, err := os.Open(directory)
	if err != nil {
		return
	}

	defer dir.Close()

	_ = dir.Sync()
}
