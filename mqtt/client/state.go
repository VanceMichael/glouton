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
	"errors"
	"sync"
	"time"

	"github.com/bleemeo/glouton/logger"
	"github.com/bleemeo/glouton/types"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// ReloadState implements the types.PahoWrapper interface.
type ReloadState struct {
	l                     sync.Mutex
	client                paho.Client
	connectionLostChannel chan error
	// stopped is closed by Close() to unblock any in-flight callback.
	// The data channels are never closed (a callback fired during a reload
	// must keep its lock-free send pending until the next run consumes it).
	stopped         chan struct{}
	pendingMessages *fifo[types.Message]

	// spoolMu guards the long-lived spool attachment. The spool itself has its
	// own mutex; this one only protects the pointer and its reload transitions.
	spoolMu sync.Mutex
	spool   *mqttSpool
}

func NewReloadState() *ReloadState {
	return &ReloadState{
		connectionLostChannel: make(chan error),
		stopped:               make(chan struct{}),
		pendingMessages:       newFifo[types.Message](maxPendingMessages),
	}
}

// ApplySpool applies the spool configuration at the start of every run.
//
// The spool is attached at most once per process (on first enablement), which
// together with the agent's run barrier guarantees a single replay owner:
//   - enabled for the first time: the WAL is opened and recovered. Recovered
//     records stay on disk and reach the FIFO only through SpoolLease;
//     in-memory backlog that predates enablement is adopted (durable appended).
//   - enabled again: limits are refreshed and a previous drain is reopened.
//   - disabled: an existing spool switches to draining mode (no new persistence,
//     existing records are still replayed until they retire); without a spool,
//     behavior is exactly the historical pure-memory one.
//
// It never returns an error to the caller of client.New: persistence problems
// degrade to the in-memory path and are exposed in the diagnostic archive.
func (rs *ReloadState) ApplySpool(cfg SpoolConfig) {
	rs.spoolMu.Lock()
	defer rs.spoolMu.Unlock()

	if !cfg.Enabled {
		if rs.spool != nil {
			rs.spool.disable()
		}

		return
	}

	if rs.spool == nil {
		s, _, err := openSpool(context.Background(), cfg, nil)
		if err != nil {
			if errors.Is(err, errSpoolLocked) {
				// Keep the locked stub so diagnostics can report the contention;
				// no writes or replay are possible through it.
				rs.spool = s

				logger.V(1).Printf("MQTT spool %s is locked by another process, falling back to in-memory pending messages", cfg.Name)
			} else {
				logger.V(1).Printf("MQTT spool %s could not be opened (%v), falling back to in-memory pending messages", cfg.Name, err)
			}

			return
		}

		rs.spool = s
		rs.adoptInMemoryBacklogLocked(s)

		logger.V(2).Printf("MQTT spool %s enabled (limit %d bytes, max age %s)", cfg.Name, cfg.MaxSizeBytes, cfg.MaxAge)

		return
	}

	if err := rs.spool.enable(cfg); err != nil {
		logger.V(1).Printf("MQTT spool %s could not be re-enabled (%v)", cfg.Name, err)
	}
}

// adoptInMemoryBacklogLocked durably appends FIFO messages that predate the
// spool (the disable->enable reload case). Already spooled messages keep their
// seq; non-retry messages are never persisted.
func (rs *ReloadState) adoptInMemoryBacklogLocked(s *mqttSpool) {
	msgs := rs.pendingMessages.Drain()
	if len(msgs) == 0 {
		return
	}

	var unleaseOnRequeueFailure []uint64

	defer func() {
		// The number of elements is unchanged, so every PutNoWait succeeds; if
		// it ever does not, give the durable lease back so a later run replays it.
		for _, msg := range msgs {
			if ok := rs.pendingMessages.PutNoWait(msg); !ok && msg.SpoolSeq != 0 {
				unleaseOnRequeueFailure = append(unleaseOnRequeueFailure, msg.SpoolSeq)
			}
		}

		if len(unleaseOnRequeueFailure) > 0 {
			s.Unlease(unleaseOnRequeueFailure...)
		}
	}()

	for i := range msgs {
		if !msgs[i].Retry || msgs[i].SpoolSeq != 0 {
			continue
		}

		seq, err := s.Append(msgs[i].Topic, msgs[i].Payload, msgs[i].EnqueuedAt)
		if err == nil {
			msgs[i].SpoolSeq = seq

			continue
		}

		// On a persistence failure the message stays memory-only; the spool
		// already counted the event (disk full or write error).
		if !errors.Is(err, ErrSpoolDiskFull) && !errors.Is(err, errSpoolDisabled) {
			logger.V(1).Printf("MQTT spool adoption append failed: %v", err)
		}
	}
}

// SpoolAppend durably records a retryable message before it is published.
// A zero seq with a nil error is impossible; callers handle any error by
// publishing in memory-only mode.
func (rs *ReloadState) SpoolAppend(topic string, payload []byte, enqueuedAt time.Time) (uint64, error) {
	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil {
		return 0, errSpoolDisabled
	}

	return s.Append(topic, payload, enqueuedAt)
}

// SpoolBeforeSend reports whether a popped message must still be sent, applying
// retention and accounting for evicted records.
func (rs *ReloadState) SpoolBeforeSend(seq uint64) sendDecision {
	if seq == 0 {
		return sendOK
	}

	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil {
		return sendOK
	}

	return s.BeforeSend(seq)
}

// SpoolRetire marks a message acknowledged in the spool.
func (rs *ReloadState) SpoolRetire(seq uint64) error {
	if seq == 0 {
		return nil
	}

	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil {
		return nil
	}

	return s.Retire(seq)
}

// SpoolLease returns recovered/undispatched spool messages ready to enter the
// FIFO. It is the only replay entry point. Leases are provisional: the caller
// must SpoolUnlease every record whose FIFO insertion failed.
func (rs *ReloadState) SpoolLease(maxRecords int) []types.Message {
	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil {
		return nil
	}

	records := s.Lease(maxRecords)
	if len(records) == 0 {
		return nil
	}

	msgs := make([]types.Message, 0, len(records))

	for _, record := range records {
		msgs = append(msgs, types.Message{
			Retry:      true,
			Topic:      record.topic,
			Payload:    record.payload,
			SpoolSeq:   record.seq,
			EnqueuedAt: record.enqueuedAt,
		})
	}

	return msgs
}

// SpoolUnlease returns leased records to the undispatched set after an
// enqueue failure, so the next run re-dispatches them without duplication.
func (rs *ReloadState) SpoolUnlease(seqs ...uint64) {
	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil || len(seqs) == 0 {
		return
	}

	s.Unlease(seqs...)
}

// SpoolSnapshot returns the observable spool state, if persistence was ever enabled.
func (rs *ReloadState) SpoolSnapshot() (SpoolStats, bool) {
	rs.spoolMu.Lock()
	s := rs.spool
	rs.spoolMu.Unlock()

	if s == nil {
		return SpoolStats{Enabled: false}, false
	}

	return s.Snapshot(), true
}

func (rs *ReloadState) Client() paho.Client {
	rs.l.Lock()
	defer rs.l.Unlock()

	client := rs.client

	return client
}

func (rs *ReloadState) SetClient(cli paho.Client) {
	rs.l.Lock()
	defer rs.l.Unlock()

	rs.client = cli
}

func (rs *ReloadState) OnConnectionLost(_ paho.Client, err error) {
	// Don't hold rs.l while sending: paho may call this during the reload
	// window when no consumer is running. Blocking here without the lock lets
	// the next run construct its client (which takes rs.l) and drain the event.
	select {
	case rs.connectionLostChannel <- err:
	case <-rs.stopped:
	}
}

func (rs *ReloadState) ConnectionLostChannel() <-chan error {
	return rs.connectionLostChannel
}

func (rs *ReloadState) AddPendingMessage(ctx context.Context, m types.Message, shouldWait bool) bool {
	if shouldWait {
		return rs.pendingMessages.Put(ctx, m)
	}

	return rs.pendingMessages.PutNoWait(m)
}

func (rs *ReloadState) PendingMessage(ctx context.Context) (m types.Message, open bool) {
	return rs.pendingMessages.Get(ctx)
}

func (rs *ReloadState) PendingMessagesCount() int {
	return rs.pendingMessages.Len()
}

func (rs *ReloadState) Close() {
	select {
	case <-rs.stopped:
		return // Close is never called twice, but do an extra check to be extra sure
	default:
	}

	// Unblock any in-flight callback (the data channel is never closed, so
	// callbacks can't panic on a send) before disconnecting.
	close(rs.stopped)

	if rs.client != nil {
		rs.client.Disconnect(uint(5 * time.Second.Milliseconds())) //nolint:gosec // constant value, always positive

		logger.V(2).Printf("Stopped MQTT with %d messages still pending", rs.pendingMessages.Len())
	}

	rs.pendingMessages.Close()

	// Flush and release the spool WAL: every live (unacknowledged) record stays
	// on disk and is replayed on the next process start as a QoS 1 duplicate.
	rs.spoolMu.Lock()
	spool := rs.spool
	rs.spoolMu.Unlock()

	if spool != nil {
		if err := spool.Close(); err != nil {
			logger.V(1).Printf("Failed to close MQTT spool: %v", err)
		}
	}
}
