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

package agent

import (
	"context"
	"path/filepath"

	"github.com/bleemeo/glouton/store/tsdb"
	"github.com/bleemeo/glouton/types"
)

// setupLocalTSDB applies the resolved local_store policy to the TSDB
// manager held by the reload state. The manager owns the current (and any
// recovered) generation across reloads.
//
// Resolution rule: an explicit agent.local_store.enable always wins;
// when unset, the store is enabled iff bleemeo.enable is false (i.e.
// when no SaaS backend will retain data, persist locally by default).
//
// The new policy is only committed once the candidate TSDB opened and
// the generation switch completed: when the open fails, the previous
// still-usable generation is retained or the manager enters recovery.
// The returned error is surfaced as a configuration warning.
func (a *agent) setupLocalTSDB() error {
	cfg := a.config.Agent.LocalStore

	enabled := !a.config.Bleemeo.Enable
	if cfg.Enable != nil {
		enabled = *cfg.Enable
	}

	path := cfg.Path
	if path == "" {
		path = filepath.Join(a.stateDir, "tsdb")
	}

	return a.reloadState.LocalStore().Apply(enabled, tsdb.Options{
		Path:      path,
		Retention: cfg.Retention,
	})
}

// teePointPusher forwards every PushPoints call to two underlying
// pushers. It is used to mirror the registry output to both the
// in-memory store (consumed by the Bleemeo connector) and the local TSDB
// manager (consumed by the local API).
type teePointPusher struct {
	primary   types.PointPusher
	secondary types.PointPusher
}

func (t teePointPusher) PushPoints(ctx context.Context, points []types.MetricPoint) {
	t.primary.PushPoints(ctx, points)
	t.secondary.PushPoints(ctx, points)
}
