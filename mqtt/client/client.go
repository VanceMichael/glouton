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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bleemeo/glouton/crashreport"
	"github.com/bleemeo/glouton/delay"
	"github.com/bleemeo/glouton/logger"
	"github.com/bleemeo/glouton/types"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	// The minimum duration to wait before reconnecting.
	minimalDelayBetweenConnect = 5 * time.Second
	// The maximum duration to wait before reconnecting.
	maximalDelayBetweenConnect = 10 * time.Minute
	// The maximum number of messages to keep while MQTT is unreachable.
	maxPendingMessages = 1000
	// If we stayed connected to MQTT for more stableConnection, the connection is considered stable,
	// and we won't wait long to reconnect in case of a disconnection.
	stableConnection    = 5 * time.Minute
	maxDelayWithoutPing = 90 * time.Second
	// How long we wait for the CONNACK. paho defaults to 30 seconds, which is too
	// short when every agent reconnects at once (e.g. a certificate rotation): the
	// broker then takes longer to answer, we give up on a connection it has already
	// accepted, and reconnecting doubles its load precisely when it is struggling.
	connectTimeout = 90 * time.Second
	// Backstop for a CONNECT token that never completes at all. connectTimeout is
	// the one that should normally fire, so this must stay above it.
	connectDeadline = 2 * time.Minute
	// After losing a stable connection, wait for a random duration in [0, maxReconnectSpread[
	// before reconnecting.
	// When every agent gets disconnected at the same time (e.g. a broker restart or a certificate
	// renewal), they would otherwise all reconnect within the same instant, and each reconnection
	// costs an authentication and a few authorizations on the server side.
	// This must stay well below the delay after which a disconnected agent is reported as such.
	maxReconnectSpread = 15 * time.Second
)

var ErrPayloadTooLarge = errors.New("payload is too large")

type Client struct {
	opts           Options
	connectionLost chan any
	stats          *mqttStats
	disableNotify  chan any
	encoder        *encoder

	l    sync.Mutex
	mqtt paho.Client

	lastConnectionTimes []time.Time
	currentConnectDelay time.Duration
	consecutiveErrors   int
	lastReport          time.Time
	disabledUntil       time.Time
	// spoolEncodeFailures counts payloads that could not be encoded and were
	// never handed to the spool or paho; exposed in the spool diagnostic file.
	spoolEncodeFailures atomic.Int64
}

type Options struct {
	// OptionsFunc returns the options to use when connecting to MQTT.
	OptionsFunc func(ctx context.Context) (*paho.ClientOptions, error)
	// Keep a state between reloads.
	ReloadState types.MQTTReloadState
	// Function called when too many errors happened.
	TooManyErrorsHandler func(ctx context.Context)
	// Function called when the broker refused our credentials.
	AuthenticationErrorHandler func(ctx context.Context)
	// A unique identifier for this client.
	ID                  string
	PahoLastPingCheckAt func() time.Time
	// SpoolEnabled turns on the persistent pending-message queue for retryable
	// messages; spool files live under SpoolDirectory in an mqtt-spool subfolder.
	SpoolEnabled   bool
	SpoolDirectory string
	SpoolMaxSize   int64
	SpoolMaxAge    time.Duration
}

// New creates a new client.
func New(opts Options) *Client {
	client := &Client{
		opts:           opts,
		mqtt:           opts.ReloadState.Client(),
		stats:          newMQTTStats(),
		disableNotify:  make(chan any),
		connectionLost: make(chan any),
		encoder:        &encoder{},
	}

	if rs, ok := opts.ReloadState.(*ReloadState); ok {
		rs.ApplySpool(SpoolConfig{
			Enabled:      opts.SpoolEnabled,
			Name:         spoolNameFromID(opts.ID),
			Directory:    opts.SpoolDirectory,
			MaxSizeBytes: opts.SpoolMaxSize,
			MaxAge:       opts.SpoolMaxAge,
		})
	}

	return client
}

func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer crashreport.ProcessPanic()
		defer wg.Done()

		c.connectionManager(ctx)
	}()

	wg.Add(1)

	go func() {
		defer crashreport.ProcessPanic()
		defer wg.Done()

		c.ackManager(ctx)
	}()

	wg.Go(func() {
		defer crashreport.ProcessPanic()

		c.receiveEvents(ctx)
	})

	wg.Go(func() {
		defer crashreport.ProcessPanic()

		c.spoolFeeder(ctx)
	})

	wg.Wait()

	// Keep the client during reloads to avoid closing the connection.
	c.opts.ReloadState.SetClient(c.mqtt)
}

func (c *Client) setupMQTT(ctx context.Context) (paho.Client, error) {
	opts, err := c.opts.OptionsFunc(ctx)
	if err != nil {
		return nil, err
	}

	// Allow for slightly larger timeout value, to avoid disconnection
	// with bad network connection.
	opts.SetPingTimeout(20 * time.Second)
	opts.SetKeepAlive(45 * time.Second)
	opts.SetConnectTimeout(connectTimeout)

	// We use our own automatic reconnection logic which is more reliable.
	opts.SetAutoReconnect(false)
	opts.SetConnectionLostHandler(c.opts.ReloadState.OnConnectionLost)

	return paho.NewClient(opts), err
}

// PublishAsJSON sends the payload to MQTT on the given topic after encoding it as JSON.
// If retry is set to true and MQTT is currently unreachable, the client will
// retry to send the message later, else it will be dropped.
func (c *Client) PublishAsJSON(topic string, payload any, retry bool) error {
	payloadBuffer, err := c.encoder.EncodeObject(payload)
	if err != nil {
		c.encoder.PutBuffer(payloadBuffer)
		c.spoolEncodeFailures.Add(1)

		return err
	}

	return c.publishWrapper(context.Background(), topic, payloadBuffer, retry)
}

// PublishBytes sends the payload to MQTT on the given topic.
// If retry is set to true and MQTT is currently unreachable, the client will
// retry to send the message later, else it will be dropped.
func (c *Client) PublishBytes(ctx context.Context, topic string, payload []byte, retry bool) error {
	payloadBuffer, err := c.encoder.EncodeBytes(payload)
	if err != nil {
		c.encoder.PutBuffer(payloadBuffer)
		c.spoolEncodeFailures.Add(1)

		return err
	}

	return c.publishWrapper(ctx, topic, payloadBuffer, retry)
}

func (c *Client) publishWrapper(ctx context.Context, topic string, payload []byte, retry bool) error {
	if len(payload) > types.MaxMQTTPayloadSize {
		c.encoder.PutBuffer(payload)

		return fmt.Errorf("%w: size is %d which is > %d", ErrPayloadTooLarge, len(payload), types.MaxMQTTPayloadSize)
	}

	c.l.Lock()
	mqtt := c.mqtt
	c.l.Unlock()

	// Non-retry messages with no live connection keep their historical
	// fire-and-forget behavior: they are neither persisted nor enqueued.
	if mqtt == nil && !retry {
		c.encoder.PutBuffer(payload)

		return nil
	}

	msg := types.Message{
		Retry:      retry,
		Payload:    payload,
		Topic:      topic,
		EnqueuedAt: time.Now(),
	}

	// Persistence happens BEFORE paho.Publish: if the process dies from this
	// point until the broker PUBACK, the record is replayed on next start
	// (MQTT QoS 1: at least once, duplicates are possible).
	if retry {
		if rs := c.concreteReloadState(); rs != nil {
			if seq, err := rs.SpoolAppend(topic, payload, msg.EnqueuedAt); err == nil {
				msg.SpoolSeq = seq
			} else if !errors.Is(err, errSpoolDisabled) {
				// Disk full or write error: don't blackhole the message,
				// publish it memory-only; the event is counted in the spool.
				logger.V(1).Printf("%s MQTT spool persistence unavailable for message on %s: %v", c.opts.ID, topic, err)
			}
		}
	}

	if mqtt != nil {
		msg.Token = mqtt.Publish(topic, 1, false, payload)
		c.stats.messagePublished(msg.Token, time.Now())
	}

	// If the enqueue fails (caller context canceled during a blocking Put),
	// the spool lease must be released so the record stays dispatchable on the
	// next run: the FIFO never received the message.
	enqueued := c.opts.ReloadState.AddPendingMessage(ctx, msg, true)
	if !enqueued && msg.SpoolSeq != 0 {
		if rs := c.concreteReloadState(); rs != nil {
			rs.SpoolUnlease(msg.SpoolSeq)
		}
	}

	return nil
}

// concreteReloadState returns the concrete *ReloadState when the interface is
// backed by one (both the open-source and Bleemeo clients do so), nil otherwise.
func (c *Client) concreteReloadState() *ReloadState {
	if rs, ok := c.opts.ReloadState.(*ReloadState); ok {
		return rs
	}

	return nil
}

// isAuthenticationError tells whether the broker refused the connection because of our credentials.
func isAuthenticationError(err error) bool {
	return errors.Is(err, packets.ErrorRefusedNotAuthorised) || errors.Is(err, packets.ErrorRefusedBadUsernameOrPassword)
}

func (c *Client) onConnectionLost(err error) {
	logger.Printf("%s MQTT connection lost: %v", c.opts.ID, err)

	c.connectionLost <- nil
}

func (c *Client) connectionManager(ctx context.Context) { //nolint:maintidx
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var lastConnectionTimes []time.Time

	currentConnectDelay := minimalDelayBetweenConnect / 2
	consecutiveError := 0
	pingMissingConsecutive := 0

mainLoop:
	for ctx.Err() == nil {
		c.l.Lock()
		c.lastConnectionTimes = lastConnectionTimes
		c.currentConnectDelay = currentConnectDelay
		c.consecutiveErrors = consecutiveError

		mqtt := c.mqtt
		disabledUntil := c.disabledUntil
		c.l.Unlock()

		switch {
		case time.Now().Before(disabledUntil):
			c.l.Lock()

			if c.mqtt != nil {
				c.mqtt.Disconnect(0)

				c.mqtt = nil
			}

			c.l.Unlock()
		case mqtt == nil:
			length := len(lastConnectionTimes)

			if length >= 7 && time.Since(lastConnectionTimes[length-7]) < 10*time.Minute {
				disableDuration := delay.JitterDelay(5*time.Minute, 0.25).Round(time.Second)

				c.Disable(time.Now().Add(disableDuration))
				logger.Printf("Too many attempts to connect to %s MQTT were made in the last 10 minutes. Disabling MQTT for %v", c.opts.ID, disableDuration)

				if c.opts.TooManyErrorsHandler != nil {
					c.opts.TooManyErrorsHandler(ctx)
				}

				continue
			}

			if length == 0 || time.Since(lastConnectionTimes[length-1]) > currentConnectDelay {
				lastConnectionTimes = append(lastConnectionTimes, time.Now())

				if len(lastConnectionTimes) > 20 {
					lastConnectionTimes = lastConnectionTimes[len(lastConnectionTimes)-20:]
				}

				if currentConnectDelay < maximalDelayBetweenConnect {
					consecutiveError++

					currentConnectDelay = delay.JitterDelay(
						delay.Exponential(minimalDelayBetweenConnect, 1.55, consecutiveError, maximalDelayBetweenConnect),
						0.1,
					)
					if consecutiveError == 5 {
						if c.opts.TooManyErrorsHandler != nil {
							c.opts.TooManyErrorsHandler(ctx)
						}
					}
				}

				mqtt, err := c.setupMQTT(ctx)
				if err != nil {
					delay := currentConnectDelay - time.Since(lastConnectionTimes[len(lastConnectionTimes)-1])
					logger.V(1).Printf("Unable to connect to %s MQTT (retry in %v): %v", c.opts.ID, delay, err)

					continue
				}

				optionReader := mqtt.OptionsReader()
				logger.V(2).Printf("Connecting to %s MQTT broker %v", c.opts.ID, optionReader.Servers()[0])

				var connectionTimeout bool

				deadline := time.Now().Add(connectDeadline)
				token := mqtt.Connect()

				for !token.WaitTimeout(1 * time.Second) {
					if ctx.Err() != nil {
						break mainLoop
					}

					if time.Now().After(deadline) {
						connectionTimeout = true

						break
					}
				}

				if token.Error() != nil || connectionTimeout {
					delay := currentConnectDelay - time.Since(lastConnectionTimes[len(lastConnectionTimes)-1])

					logger.V(1).Printf("Unable to connect to %s MQTT (retry in %v): %v", c.opts.ID, delay, token.Error())

					if isAuthenticationError(token.Error()) && c.opts.AuthenticationErrorHandler != nil {
						c.opts.AuthenticationErrorHandler(ctx)
					}

					// we must disconnect to stop paho gorouting that otherwise will be
					// started multiple time for each Connect()
					mqtt.Disconnect(0)
				} else {
					c.l.Lock()
					c.mqtt = mqtt
					c.l.Unlock()

					logger.Printf("%s MQTT connection established", c.opts.ID)
				}
			}
		case mqtt != nil && mqtt.IsConnectionOpen():
			if c.opts.PahoLastPingCheckAt != nil && !c.opts.PahoLastPingCheckAt().IsZero() && time.Since(c.opts.PahoLastPingCheckAt()) > maxDelayWithoutPing {
				pingMissingConsecutive++

				if pingMissingConsecutive >= 2 {
					logger.V(1).Println("Paho ping check seems blocked, forcing a disconnection")

					c.l.Lock()

					if c.mqtt != nil {
						c.mqtt.Disconnect(100)
					}

					c.mqtt = nil

					c.l.Unlock()
				}
			} else {
				pingMissingConsecutive = 0
			}
		}

		select {
		case <-ctx.Done():
		case <-c.connectionLost:
			c.l.Lock()

			if c.mqtt != nil {
				c.mqtt.Disconnect(0)
			}

			c.mqtt = nil
			c.l.Unlock()

			length := len(lastConnectionTimes)
			if length > 0 && time.Since(lastConnectionTimes[length-1]) > stableConnection {
				logger.V(2).Printf("%s MQTT connection was stable, reset delay to %v", c.opts.ID, minimalDelayBetweenConnect)
				currentConnectDelay = minimalDelayBetweenConnect
				consecutiveError = 0

				// The connection was stable, so the next iteration would reconnect at once.
				// Wait a bit first, so that agents disconnected together don't reconnect together.
				spread := delay.JitterMs(maxReconnectSpread/2, 1)
				logger.V(2).Printf("Reconnecting to %s MQTT in %v", c.opts.ID, spread)

				select {
				case <-time.After(spread):
				case <-ctx.Done():
				}
			} else if length > 0 {
				delay := currentConnectDelay - time.Since(lastConnectionTimes[len(lastConnectionTimes)-1])
				if delay > 0 {
					logger.V(1).Printf("Retry to connection to %s MQTT in %v", c.opts.ID, delay)
				}
			}
		case <-c.disableNotify:
		case <-ticker.C:
		}
	}

	// Make sure all connection lost events are read.
	for {
		select {
		case <-c.connectionLost:
		default:
			return
		}
	}
}

func (c *Client) ackManager(ctx context.Context) {
	var lastErrShowed time.Time

	for ctx.Err() == nil {
		msg, open := c.opts.ReloadState.PendingMessage(ctx)
		if !open {
			logger.V(2).Println("MQTT messages queue has been closed.")

			<-ctx.Done()

			break
		}

		err := c.ackOne(msg, 10*time.Second)
		if err != nil {
			if time.Since(lastErrShowed) > time.Minute {
				logger.V(2).Printf(
					"%s MQTT publish on %s failed: %v (%d pending messages)",
					c.opts.ID, msg.Topic, err, c.opts.ReloadState.PendingMessagesCount(),
				)

				lastErrShowed = time.Now()
			}
		}
	}
}

// spoolDispatchBatchSize bounds how many recovered records a feeder tick may
// move into the in-memory FIFO.
const spoolDispatchBatchSize = 64

// spoolDispatchInterval is how often the feeder looks for recovered records.
const spoolDispatchInterval = 200 * time.Millisecond

// spoolFeeder is the unique replayer of recovered spool records. It moves
// undispatched live records into the pending FIFO with backpressure (Put blocks
// when the FIFO is full), where the ack manager republishes them with QoS 1.
func (c *Client) spoolFeeder(ctx context.Context) {
	rs := c.concreteReloadState()
	if rs == nil {
		return
	}

	ticker := time.NewTicker(spoolDispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		msgs := rs.SpoolLease(spoolDispatchBatchSize)

		for i, msg := range msgs {
			// Token is nil: the record is published by the ack manager like any
			// message queued while the broker was unreachable.
			if enqueued := rs.AddPendingMessage(ctx, msg, true); !enqueued {
				// The run ended (reload/shutdown) before this record (and the
				// following ones) entered the FIFO: give the leases back so the
				// next run re-dispatches them instead of stranding them as queued.
				rs.SpoolUnlease(spoolMessageSeqs(msgs[i:])...)

				return
			}
		}
	}
}

func spoolMessageSeqs(msgs []types.Message) []uint64 {
	seqs := make([]uint64, len(msgs))
	for i, msg := range msgs {
		seqs[i] = msg.SpoolSeq
	}

	return seqs
}

func (c *Client) ackOne(msg types.Message, timeout time.Duration) error {
	// Records explicitly evicted by the retention/size policy, or unknown to the
	// spool (already evicted), must never be (re)sent: explicit eviction is the
	// only case besides acknowledgement that removes a message.
	if msg.SpoolSeq != 0 {
		if rs := c.concreteReloadState(); rs != nil {
			switch rs.SpoolBeforeSend(msg.SpoolSeq) {
			case sendDropExpired:
				logger.V(2).Printf("%s MQTT dropping expired pending message on %s (older than retention)", c.opts.ID, msg.Topic)
				c.encoder.PutBuffer(msg.Payload)

				return nil
			case sendDropUnknown:
				logger.V(2).Printf("%s MQTT dropping evicted pending message on %s", c.opts.ID, msg.Topic)
				c.encoder.PutBuffer(msg.Payload)

				return nil
			case sendRetryEviction:
				// The eviction could not be persisted: keep payload ownership and
				// requeue the message so the eviction is retried once I/O recovers.
				logger.V(1).Printf("%s MQTT age eviction of message on %s could not be persisted, retrying", c.opts.ID, msg.Topic)
				c.opts.ReloadState.AddPendingMessage(context.Background(), msg, true)
				time.Sleep(time.Second)

				return nil
			case sendOK:
			}
		}
	}

	var err error

	// The token is nil when publishing failed.
	shouldWaitForACKAgain := false
	if msg.Token != nil {
		shouldWaitForACKAgain = !msg.Token.WaitTimeout(timeout)

		err = msg.Token.Error()
		if err != nil {
			c.stats.ackFailed(msg.Token)
		}
	}

	// Retry publishing the message if there was an error.
	if msg.Token == nil || msg.Token.Error() != nil {
		if !msg.Retry {
			c.encoder.PutBuffer(msg.Payload)

			return err
		}

		c.l.Lock()
		mqtt := c.mqtt
		c.l.Unlock()

		if mqtt != nil {
			msg.Token = mqtt.Publish(msg.Topic, 1, false, msg.Payload)
		}

		publishFailed := mqtt == nil || msg.Token.Error() != nil

		// The Token will be awaited later.
		shouldWaitForACKAgain = true

		// It's possible for Publish to return instantly with an error,
		// in this case we need to wait a bit to avoid consuming too much resources.
		if publishFailed {
			msg.Token = nil

			time.Sleep(time.Second)
		}
	}

	if shouldWaitForACKAgain { // Note: we don't care about the context here: we won't wait anyway.
		ok := c.opts.ReloadState.AddPendingMessage(context.Background(), msg, false)

		if !ok {
			logger.V(1).Printf("%s: MQTT pending message queue is full, message on topic %s is dropped", c.opts.ID, msg.Topic)
		}

		return err
	}

	// The broker acknowledged the PUBLISH: only now may the spool record retire.
	// If the durable RETIRE cannot be written, keep the message queued so the
	// retirement is retried: the record must not vanish before it is retired.
	if msg.SpoolSeq != 0 {
		if rs := c.concreteReloadState(); rs != nil {
			if retireErr := rs.SpoolRetire(msg.SpoolSeq); retireErr != nil {
				logger.V(1).Printf("%s MQTT broker acknowledged message on %s but spool retirement failed, retrying: %v", c.opts.ID, msg.Topic, retireErr)

				c.opts.ReloadState.AddPendingMessage(context.Background(), msg, true)

				return retireErr
			}
		}
	}

	now := time.Now()

	c.encoder.PutBuffer(msg.Payload)
	c.stats.ackReceived(msg.Token, now)

	c.l.Lock()
	defer c.l.Unlock()

	c.lastReport = now

	return nil
}

func (c *Client) receiveEvents(ctx context.Context) {
	for {
		select {
		case err := <-c.opts.ReloadState.ConnectionLostChannel():
			c.onConnectionLost(err)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) IsConnectionOpen() bool {
	c.l.Lock()
	defer c.l.Unlock()

	return c.isConnectionOpen()
}

func (c *Client) isConnectionOpen() bool {
	if c.mqtt == nil {
		return false
	}

	return c.mqtt.IsConnectionOpen()
}

// DiagnosticArchive add to a zipfile useful diagnostic information.
func (c *Client) DiagnosticArchive(_ context.Context, archive types.ArchiveWriter) error {
	c.l.Lock()
	defer c.l.Unlock()

	fileID := strings.ToLower(strings.ReplaceAll(c.opts.ID, " ", "-"))

	file, err := archive.Create(fileID + "-mqtt-stats.txt")
	if err != nil {
		return err
	}

	fmt.Fprintf(file, "%s", c.stats)

	file, err = archive.Create(fileID + "-mqtt-client.json")
	if err != nil {
		return err
	}

	obj := struct {
		ConnectionOpen      bool
		PendingMessageCount int
		LastConnectionTimes []time.Time
		CurrentConnectDelay string
		ConsecutiveErrors   int
		LastReport          time.Time
		DisabledUntil       time.Time
	}{
		ConnectionOpen:      c.isConnectionOpen(),
		PendingMessageCount: c.opts.ReloadState.PendingMessagesCount(),
		LastConnectionTimes: c.lastConnectionTimes,
		CurrentConnectDelay: c.currentConnectDelay.String(),
		ConsecutiveErrors:   c.consecutiveErrors,
		LastReport:          c.lastReport,
		DisabledUntil:       c.disabledUntil,
	}

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")

	if err := enc.Encode(obj); err != nil {
		return err
	}

	file, err = archive.Create(fileID + "-mqtt-spool.json")
	if err != nil {
		return err
	}

	spoolObj := struct {
		EncodeFailedEvents int64 `json:"encode_failed_events"`
		SpoolStats
	}{
		EncodeFailedEvents: c.spoolEncodeFailures.Load(),
	}

	if rs := c.concreteReloadState(); rs != nil {
		if stats, ok := rs.SpoolSnapshot(); ok {
			spoolObj.SpoolStats = stats
		}
	}

	spoolEnc := json.NewEncoder(file)
	spoolEnc.SetIndent("", "  ")

	return spoolEnc.Encode(spoolObj)
}

// LastReport returns the date of last acknowledgment received.
func (c *Client) LastReport() time.Time {
	c.l.Lock()
	defer c.l.Unlock()

	return c.lastReport
}

// Disable will disable the MQTT connection until given time.
// To re-enable use ClearDisable().
func (c *Client) Disable(until time.Time) {
	c.l.Lock()
	defer c.l.Unlock()

	c.disabledUntil = until

	if c.disableNotify != nil {
		select {
		case c.disableNotify <- nil:
		default:
		}
	}
}

// ForceReconnect drops the current connection so that the client establishes a
// new one. Used when the server certificate changed: the connection is still
// usable, but it is pinned to the old certificate, and Envoy will close it for
// us at the end of its drain period - every connection at the same moment.
// Reconnecting on request lets that be spread out instead.
//
// The connection manager re-reads c.mqtt on every iteration, so clearing it here
// is enough: the reconnection happens on the next tick.
func (c *Client) ForceReconnect() {
	c.l.Lock()
	defer c.l.Unlock()

	if c.mqtt == nil {
		return
	}

	logger.V(1).Printf("%s MQTT reconnection requested", c.opts.ID)

	c.mqtt.Disconnect(100)
	c.mqtt = nil
}

func (c *Client) DisabledUntil() time.Time {
	c.l.Lock()
	defer c.l.Unlock()

	return c.disabledUntil
}

func (c *Client) Disconnect(timeout time.Duration) {
	c.l.Lock()
	defer c.l.Unlock()

	if c.mqtt != nil {
		c.mqtt.Disconnect(uint(timeout.Milliseconds())) //nolint:gosec // timeout is always non-negative
	}

	c.mqtt = nil
}
