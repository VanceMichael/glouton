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

package config

import (
	"strings"
	"testing"
	"time"
)

func TestMQTTSpoolDefault(t *testing.T) {
	t.Parallel()

	// A configuration without any spool section must receive the default
	// spool values for both MQTT connectors (backward compatibility).
	cfg, _, warnings, err := Load(true, false, "testdata/simple.conf")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}

	want := DefaultMQTTSpool()

	if got := cfg.MQTT.Spool; got != want {
		t.Errorf("mqtt.spool = %+v, want default %+v", got, want)
	}

	if got := cfg.Bleemeo.MQTT.Spool; got != want {
		t.Errorf("bleemeo.mqtt.spool = %+v, want default %+v", got, want)
	}
}

func TestMQTTSpoolCustom(t *testing.T) {
	t.Parallel()

	cfg, _, warnings, err := Load(true, false, "testdata/mqtt-spool.conf")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", warnings)
	}

	if !cfg.MQTT.Spool.Enable || cfg.MQTT.Spool.MaxSizeMB != 42 || cfg.MQTT.Spool.MaxAge != 48*time.Hour {
		t.Errorf("mqtt.spool = %+v, want {enable=true, max_size_mb=42, max_age=48h}", cfg.MQTT.Spool)
	}

	if !cfg.Bleemeo.MQTT.Spool.Enable || cfg.Bleemeo.MQTT.Spool.MaxSizeMB != 7 || cfg.Bleemeo.MQTT.Spool.MaxAge != time.Hour {
		t.Errorf("bleemeo.mqtt.spool = %+v, want {enable=true, max_size_mb=7, max_age=1h}", cfg.Bleemeo.MQTT.Spool)
	}
}

func TestMQTTSpoolInvalidValues(t *testing.T) {
	t.Parallel()

	cfg, _, warnings, err := Load(true, false, "testdata/mqtt-spool-invalid.conf")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Negative limits fall back to the defaults and must be reported as warnings.
	if got := cfg.MQTT.Spool.MaxSizeMB; got != DefaultMQTTSpoolMaxSizeMB {
		t.Errorf("mqtt.spool.max_size_mb = %d, want default %d", got, DefaultMQTTSpoolMaxSizeMB)
	}

	if got := cfg.MQTT.Spool.MaxAge; got != DefaultMQTTSpoolMaxAge {
		t.Errorf("mqtt.spool.max_age = %s, want default %s", got, DefaultMQTTSpoolMaxAge)
	}

	// Unset max_size_mb is normalized to the default without a warning.
	if got := cfg.Bleemeo.MQTT.Spool.MaxSizeMB; got != DefaultMQTTSpoolMaxSizeMB {
		t.Errorf("bleemeo.mqtt.spool.max_size_mb = %d, want default %d", got, DefaultMQTTSpoolMaxSizeMB)
	}

	// An explicit zero duration disables age-based eviction and is kept.
	if got := cfg.Bleemeo.MQTT.Spool.MaxAge; got != 0 {
		t.Errorf("bleemeo.mqtt.spool.max_age = %s, want 0 (age eviction disabled)", got)
	}

	gotWarnings := ""
	for _, w := range warnings {
		gotWarnings += w.Error() + "\n"
	}

	for _, want := range []string{"mqtt.spool.max_size_mb", "mqtt.spool.max_age"} {
		if !strings.Contains(gotWarnings, want) {
			t.Errorf("missing warning for %s, got warnings:\n%s", want, gotWarnings)
		}
	}

	if strings.Contains(gotWarnings, "bleemeo.mqtt.spool") {
		t.Errorf("the explicit bleemeo zero duration must not warn, got:\n%s", gotWarnings)
	}
}
