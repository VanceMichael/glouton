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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bleemeo/glouton/config"
	"github.com/bleemeo/glouton/mqtt/client"
	"github.com/bleemeo/glouton/types"
)

func TestOpenMQTTSpoolWiring(t *testing.T) {
	t.Parallel()

	// Enabled: constructing the connector creates the spool directory and files
	// under the agent state directory, even before the first run.
	dir := t.TempDir()
	rs := client.NewReloadState()

	_ = New(Options{
		ReloadState:    rs,
		StateDirectory: dir,
		FQDN:           "agent1",
		Config: config.OpenSourceMQTT{
			Enable: true,
			Hosts:  []string{"127.0.0.1"},
			Spool: config.MQTTSpool{
				Enable:    true,
				MaxSizeMB: 4,
				MaxAge:    2 * time.Hour,
			},
		},
	})

	spoolDir := filepath.Join(dir, "mqtt-spool")

	for _, name := range []string{"open-source.wal", "open-source.lock"} {
		if _, err := os.Stat(filepath.Join(spoolDir, name)); err != nil {
			t.Errorf("expected %s to be created: %v", name, err)
		}
	}

	rs.Close()

	// Disabled (default): no spool file or directory is ever created.
	dirDisabled := t.TempDir()
	rsDisabled := client.NewReloadState()

	_ = New(Options{
		ReloadState:    rsDisabled,
		StateDirectory: dirDisabled,
		FQDN:           "agent2",
		Config: config.OpenSourceMQTT{
			Enable: true,
			Hosts:  []string{"127.0.0.1"},
			Spool:  config.MQTTSpool{}, // normalized defaults, enable false
		},
	})

	if _, err := os.Stat(filepath.Join(dirDisabled, "mqtt-spool")); !os.IsNotExist(err) {
		t.Fatalf("mqtt-spool directory must not exist when the spool is disabled (err=%v)", err)
	}

	rsDisabled.Close()
}

// TestOpenMQTTBatchingPreserved ensures the 1000-points-per-batch publication
// behavior is unchanged (broker unreachable: every batch still enters the
// pending queue, none is lost).
func TestOpenMQTTBatchingPreserved(t *testing.T) {
	t.Parallel()

	rs := client.NewReloadState()
	t.Cleanup(func() { rs.Close() })

	m := New(Options{
		ReloadState:    rs,
		StateDirectory: t.TempDir(),
		FQDN:           "agent1",
		Config:         config.OpenSourceMQTT{Enable: true, Hosts: []string{"127.0.0.1"}},
	})

	const totalPoints = pointsBatchSize*2 + 1

	points := make([]types.MetricPoint, totalPoints)
	for i := range points {
		points[i] = types.MetricPoint{
			Point: types.Point{Time: time.Now(), Value: float64(i)},
			Labels: map[string]string{
				types.LabelName: "cpu_used",
				types.LabelItem: "agent1",
			},
		}
	}

	m.addPoints(points)
	m.sendPoints()

	if got := rs.PendingMessagesCount(); got != 3 {
		t.Fatalf("pending messages = %d, want 3 batches of %d points", got, pointsBatchSize)
	}
}
