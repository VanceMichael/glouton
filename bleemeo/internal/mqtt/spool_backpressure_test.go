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

package mqtt

import (
	"context"
	"testing"
	"time"

	bleemeoTypes "github.com/bleemeo/glouton/bleemeo/types"
	gloutonTypes "github.com/bleemeo/glouton/types"

	"github.com/bleemeo/glouton/mqtt/client"
)

// bleemeoReloadStateStub is a BleemeoReloadState exposing only the MQTT state.
type bleemeoReloadStateStub struct {
	bleemeoTypes.BleemeoReloadState
	mqttState bleemeoTypes.MQTTReloadState
}

func (s *bleemeoReloadStateStub) MQTTReloadState() bleemeoTypes.MQTTReloadState {
	return s.mqttState
}

// bleemeoMQTTReloadStateStub exposes only ClientState (the shared client layer).
type bleemeoMQTTReloadStateStub struct {
	bleemeoTypes.MQTTReloadState
	clientState gloutonTypes.MQTTReloadState
}

func (s *bleemeoMQTTReloadStateStub) ClientState() gloutonTypes.MQTTReloadState {
	return s.clientState
}

// TestCanSendLogsBackPressureWithBacklog verifies that logs stay back-pressured
// while the pending queue (which also carries spool-recovered messages) exceeds
// the threshold, and recover as soon as it drains — preserving the existing
// behavior with the new persistence layer underneath.
func TestCanSendLogsBackPressureWithBacklog(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := client.NewReloadState()
	t.Cleanup(inner.Close)

	rs := &bleemeoReloadStateStub{
		mqttState: &bleemeoMQTTReloadStateStub{clientState: inner},
	}

	c := &Client{
		opts:    Option{GlobalOption: bleemeoTypes.GlobalOption{ReloadState: rs}},
		lastAck: time.Now(),
	}

	// Threshold is maxMQTTPendingMessagesToAllowLogsThreshold (50).
	for range maxMQTTPendingMessagesToAllowLogsThreshold + 1 {
		inner.AddPendingMessage(ctx, gloutonTypes.Message{Retry: true, Topic: "v1/data"}, true)
	}

	if c.canSendLogs() {
		t.Fatal("canSendLogs = true with 51 pending messages, want back-pressure")
	}

	// Drain the queue: the gate opens again.
	for range maxMQTTPendingMessagesToAllowLogsThreshold + 1 {
		if _, open := inner.PendingMessage(ctx); !open {
			t.Fatal("pending queue closed unexpectedly")
		}
	}

	if !c.canSendLogs() {
		t.Fatal("canSendLogs = false with an empty queue and a recent ack")
	}
}

// TestCanSendLogsBackPressureWithStaleAck verifies ack staleness still gates logs
// independently of the queue size.
func TestCanSendLogsBackPressureWithStaleAck(t *testing.T) {
	t.Parallel()

	inner := client.NewReloadState()
	t.Cleanup(inner.Close)

	rs := &bleemeoReloadStateStub{
		mqttState: &bleemeoMQTTReloadStateStub{clientState: inner},
	}

	// A very old lastAck trips dataAckBackPressureDelay even with an empty queue.
	c := &Client{
		opts:    Option{GlobalOption: bleemeoTypes.GlobalOption{ReloadState: rs}},
		lastAck: time.Now().Add(-2 * logsAckBackPressureDelay),
	}

	if c.canSendLogs() {
		t.Fatal("canSendLogs = true with a stale ack, want back-pressure")
	}
}
