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

// Package promexporter implement an discovery of Prometheus exporter based on Docker labels / Kubernetes annotations
package promexporter

import (
	"fmt"
	"maps"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/bleemeo/glouton/facts"
	"github.com/bleemeo/glouton/logger"
	"github.com/bleemeo/glouton/prometheus/registry"
	"github.com/bleemeo/glouton/prometheus/scrapper"
	"github.com/bleemeo/glouton/types"

	"github.com/prometheus/prometheus/model/labels"
)

const labelValueTrue = "true"

// listExporters return list of exporters based on containers labels/annotations.
func (d *DynamicScrapper) listExporters(containers []facts.Container) []*scrapper.Target {
	result := make([]*scrapper.Target, 0)

	for _, c := range containers {
		u := urlFromLabels(c.Labels(), c.PrimaryAddress())

		if u == "" {
			u = urlFromLabels(c.Annotations(), c.PrimaryAddress())
		}

		if u == "" {
			continue
		}

		tmp, err := url.Parse(u)
		if err != nil {
			logger.Printf("ignoring invalid URL %v: %v", u, err)

			continue
		}

		labels := map[string]string{
			types.LabelMetaScrapeJob:      d.DynamicJobName,
			types.LabelMetaScrapeInstance: scrapper.HostPort(tmp),
		}

		ns := c.PodNamespace()
		podName := c.PodName()

		if podName != "" {
			labels[types.LabelK8SNamespace] = ns
			labels[types.LabelK8SPODName] = podName
		} else {
			labels[types.LabelMetaContainerName] = c.ContainerName()
		}

		cLabelsAnnotations := facts.LabelsAndAnnotations(c)

		target := &scrapper.Target{
			URL:             tmp,
			ExtraLabels:     labels,
			ContainerLabels: cLabelsAnnotations,
		}
		result = append(result, target)
	}

	return result
}

func urlFromLabels(labels map[string]string, address string) string {
	if strings.ToLower(labels["prometheus.io/scrape"]) != labelValueTrue {
		return ""
	}

	path := labels["prometheus.io/path"]
	if path == "" {
		path = "/metrics"
	}

	portStr := labels["prometheus.io/port"]
	if portStr == "" {
		portStr = "9102"
	}

	port, err := strconv.ParseInt(portStr, 10, 0)
	if err != nil {
		return ""
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return fmt.Sprintf("http://%s%s", net.JoinHostPort(address, strconv.FormatInt(port, 10)), path)
}

// registeredTarget is a discovered exporter currently registered in the registry.
type registeredTarget struct {
	registration    types.Registration
	extraLabels     map[string]string
	containerLabels map[string]string
}

// DynamicScrapper is a Prometheus scrapper that will update its target based on ListExporters.
type DynamicScrapper struct {
	l          sync.Mutex
	registered map[string]registeredTarget

	// Observability counters, guarded by l.
	incompleteSnapshots  int
	retainedTargets      int
	retiredTargets       int
	replacedIdentities   int
	registrationFailures int
	DynamicJobName       string
	Registry             *registry.Registry
}

// Stats exposes how the dynamic scrapper reacted to discovery snapshots. All fields are
// cumulative since creation.
type Stats struct {
	// RegisteredTargets is the current number of registered exporter targets.
	RegisteredTargets int
	// IncompleteSnapshots counts non-authoritative snapshots (mayForgetAbsent=false) during
	// which at least one absent target was kept registered.
	IncompleteSnapshots int
	// RetainedTargets counts target instances kept registered because the snapshot that
	// omitted them was not authoritative.
	RetainedTargets int
	// RetiredTargets counts targets unregistered after an authoritative snapshot confirmed
	// their absence (the registry emitted stale markers for their last emitted series).
	RetiredTargets int
	// ReplacedIdentities counts targets whose scrape identity (URL labels) changed: the old
	// identity was converged (stale markers) before the new one was registered.
	ReplacedIdentities int
	// RegistrationFailures counts targets that could not be (re)registered in the registry.
	RegistrationFailures int
}

// Update updates the scrappers targets using new containers information.
//
// complete tells whether every container runtime enumerated successfully this call;
// mayForgetAbsent tells whether a container missing from the list may be treated as
// permanently removed (it is stricter than complete). When mayForgetAbsent is false,
// targets absent from the snapshot are retained with the series from their last successful
// scrape, and a later authoritative snapshot is still free to retire them. When it is true,
// absent targets are authoritatively retired. A target whose scrape URL labels changed is
// always retired under its old identity and re-registered under the new one, independently of
// mayForgetAbsent.
func (d *DynamicScrapper) Update(containers []facts.Container, complete bool, mayForgetAbsent bool) {
	d.l.Lock()
	defer d.l.Unlock()

	d.update(containers, complete, mayForgetAbsent)
}

func (d *DynamicScrapper) update(containers []facts.Container, complete bool, mayForgetAbsent bool) {
	dynamicTargets := d.listExporters(containers)

	if len(dynamicTargets) > 0 {
		dynamicTargetsStr := make([]string, 0, len(dynamicTargets))
		for _, target := range dynamicTargets {
			dynamicTargetsStr = append(dynamicTargetsStr, target.URL.String())
		}

		logger.V(3).Printf("Found the following dynamic Prometheus exporter: %v", dynamicTargetsStr)
	}

	if d.registered == nil {
		d.registered = make(map[string]registeredTarget)
	}

	currentURLs := make(map[string]bool, len(dynamicTargets))

	// Phase 1: classify the current snapshot. Targets whose labels changed at the same URL
	// are identity replacements and are converged immediately (old registration stopped and
	// stale-marked), independently of the snapshot authoritativeness.
	toRegister := make([]*scrapper.Target, 0, len(dynamicTargets))

	for _, t := range dynamicTargets {
		u := t.URL.String()
		currentURLs[u] = true

		old, ok := d.registered[u]
		if !ok {
			toRegister = append(toRegister, t)

			continue
		}

		if reflect.DeepEqual(old.extraLabels, t.ExtraLabels) {
			continue
		}

		// The scrape identity changed at the same URL: converge the old identity before the
		// new one is enabled below. Unregister stops its scrape loop and emits stale markers
		// for exactly the series it last emitted; other registrations sharing metric names
		// are unaffected.
		old.registration.Unregister()
		delete(d.registered, u)

		d.replacedIdentities++

		logger.V(1).Printf(
			"Dynamic Prometheus exporter %s changed scrape labels %v -> %v, retired old identity before registering the new one",
			u, old.extraLabels, t.ExtraLabels,
		)

		toRegister = append(toRegister, t)
	}

	// Phase 2: converge targets absent from this snapshot. An authoritative
	// (mayForgetAbsent) snapshot retires them; a possibly partial one retains the target and
	// its last successfully scraped series, so the next complete snapshot can either confirm
	// or retire it.
	retained := make([]string, 0)
	retired := make([]string, 0)

	for u, target := range d.registered {
		if currentURLs[u] {
			continue
		}

		if !mayForgetAbsent {
			retained = append(retained, u)

			continue
		}

		target.registration.Unregister()
		delete(d.registered, u)

		retired = append(retired, u)
	}

	if len(retained) > 0 {
		d.incompleteSnapshots++
		d.retainedTargets += len(retained)

		logger.V(1).Printf(
			"Keeping %d dynamic Prometheus exporter(s) absent from an incomplete container snapshot (complete=%v, mayForgetAbsent=%v), waiting for the next authoritative snapshot: %v",
			len(retained), complete, mayForgetAbsent, retained,
		)
	}

	if len(retired) > 0 {
		d.retiredTargets += len(retired)

		logger.V(1).Printf(
			"Retiring %d dynamic Prometheus exporter(s) confirmed absent by an authoritative container snapshot: %v",
			len(retired), retired,
		)
	}

	// Phase 3: enable new identities only after every old identity from this snapshot has
	// converged, so a stale generation can never race a sample from the replacement.
	for _, t := range toRegister {
		u := t.URL.String()
		hash := labels.FromMap(t.ExtraLabels).Hash()

		id, err := d.Registry.RegisterGatherer(
			registry.RegistrationOption{
				Description:              "Prometheus exporter " + u,
				InstanceUseContainerName: true,
				JitterSeed:               hash,
				Rules:                    t.Rules,
				ExtraLabels:              t.ExtraLabels,
				AcceptAllowedMetricsOnly: true,
				RetireEmittedSeries:      true,
			},
			t,
		)
		if err != nil {
			logger.Printf("Failed to register scrapper for %v: %v", t.URL, err)

			d.registrationFailures++

			continue
		}

		d.registered[u] = registeredTarget{
			registration:    id,
			extraLabels:     t.ExtraLabels,
			containerLabels: t.ContainerLabels,
		}
	}
}

// Stats returns cumulative counters describing dynamic target retention, retirement,
// identity replacement and registration failures.
func (d *DynamicScrapper) Stats() Stats {
	d.l.Lock()
	defer d.l.Unlock()

	return Stats{
		RegisteredTargets:    len(d.registered),
		IncompleteSnapshots:  d.incompleteSnapshots,
		RetainedTargets:      d.retainedTargets,
		RetiredTargets:       d.retiredTargets,
		ReplacedIdentities:   d.replacedIdentities,
		RegistrationFailures: d.registrationFailures,
	}
}

// GetRegisteredLabels returns a copy of registeredLabels.
func (d *DynamicScrapper) GetRegisteredLabels() map[string]map[string]string {
	d.l.Lock()
	defer d.l.Unlock()

	result := make(map[string]map[string]string, len(d.registered))

	for key, target := range d.registered {
		result[key] = copyStringMap(target.extraLabels)
	}

	return result
}

// GetContainersLabels returns a copy of containersLabels.
func (d *DynamicScrapper) GetContainersLabels() map[string]map[string]string {
	d.l.Lock()
	defer d.l.Unlock()

	result := make(map[string]map[string]string, len(d.registered))

	for key, target := range d.registered {
		result[key] = copyStringMap(target.containerLabels)
	}

	return result
}

func copyStringMap(toCopy map[string]string) map[string]string {
	newMap := make(map[string]string, len(toCopy))
	maps.Copy(newMap, toCopy)

	return newMap
}
