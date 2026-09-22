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

package promexporter

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bleemeo/glouton/facts"
	"github.com/bleemeo/glouton/prometheus/registry"
	"github.com/bleemeo/glouton/prometheus/scrapper"
	gloutonStore "github.com/bleemeo/glouton/store"
	gloutonTSDB "github.com/bleemeo/glouton/store/tsdb"
	"github.com/bleemeo/glouton/types"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	fakeJobName      = "jobname"
	fakePodNamespace = "default"

	testContainerMyContainer = "my_container"
	testAddressSample        = "sample"
	testInstanceSample9102   = "sample:9102"
	testPodMyPod1234         = "my_pod-1234"
	testNameTestname         = "testname"

	labelPromScrape         = "prometheus.io/scrape"
	labelPromPort           = "prometheus.io/port"
	labelPromPath           = "prometheus.io/path"
	labelGloutonAllow       = "glouton.allow_metrics"
	labelGloutonDeny        = "glouton.deny_metrics"
	labelGloutonInclDefault = "glouton.include_default_metrics"
)

func TestListExporters(t *testing.T) { //nolint:maintidx
	mustParse := func(text string) *url.URL {
		u, err := url.Parse(text)
		if err != nil {
			t.Fatal(err)
		}

		return u
	}

	tests := []struct {
		name                 string
		containers           []facts.Container
		want                 []*scrapper.Target
		globalIncludeDefault bool
	}{
		{
			name:       "empty",
			containers: []facts.Container{},
			want:       []*scrapper.Target{},
		},
		{
			name: "docker",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testContainerMyContainer,
					FakePrimaryAddress: testAddressSample,
					FakeLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testContainerMyContainer,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "k8s",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  "k8s_containername_podname_namespace",
					FakePodName:        testPodMyPod1234,
					FakePodNamespace:   fakePodNamespace,
					FakePrimaryAddress: testAddressSample,
					FakeAnnotations: map[string]string{
						labelPromScrape: labelValueTrue,
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
					ExtraLabels: map[string]string{
						// K8S don't use meta label, because registry don't convert them to normal label unlike container_name label
						types.LabelK8SNamespace:       fakePodNamespace,
						types.LabelK8SPODName:         testPodMyPod1234,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "another labels",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  "container",
					FakePrimaryAddress: testAddressSample,
					FakeLabels: map[string]string{
						"glouton.enable": labelValueTrue,
						"my_label":       "value",
					},
					FakeAnnotations: map[string]string{
						"kubernetes.io/hello": "world",
					},
				},
			},
			want: []*scrapper.Target{},
		},
		{
			name: "two-with-alternate-port",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  "sample1_1",
					FakePrimaryAddress: "sample1",
					FakeLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
				},
				facts.FakeContainer{
					FakeContainerName:  "k8s_sample2_default",
					FakePodName:        "sample2-1234",
					FakePodNamespace:   fakePodNamespace,
					FakePrimaryAddress: "sample2",
					FakeLabels: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPort:   "8080",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample1:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  "sample1_1",
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: "sample1:9102",
					},
				},
				{
					URL: mustParse("http://sample2:8080/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPort:   "8080",
					},
					ExtraLabels: map[string]string{
						types.LabelK8SNamespace:       fakePodNamespace,
						types.LabelK8SPODName:         "sample2-1234",
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: "sample2:8080",
					},
				},
			},
		},
		{
			name: "full-configured",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testNameTestname,
					FakePrimaryAddress: testAddressSample,
					FakeAnnotations: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPort:   "8080",
						labelPromPath:   "/metrics.txt",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:8080/metrics.txt"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPort:   "8080",
						labelPromPath:   "/metrics.txt",
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testNameTestname,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: "sample:8080",
					},
				},
			},
		},
		{
			name: "path-without-slash",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testNameTestname,
					FakePrimaryAddress: testAddressSample,
					FakeAnnotations: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPath:   "metrics.txt",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics.txt"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
						labelPromPath:   "metrics.txt",
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testNameTestname,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "docker-global-metrics",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testContainerMyContainer,
					FakePrimaryAddress: testAddressSample,
					FakeLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape: labelValueTrue,
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testContainerMyContainer,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "docker-allow-deny-metrics",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testContainerMyContainer,
					FakePrimaryAddress: testAddressSample,
					FakeLabels: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "cpu_used,mem_used",
						labelGloutonDeny:        "up{job=\"prometheus\"}",
						labelGloutonInclDefault: "no",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "cpu_used,mem_used",
						labelGloutonDeny:        "up{job=\"prometheus\"}",
						labelGloutonInclDefault: "no",
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testContainerMyContainer,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "docker-reset-global",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  testContainerMyContainer,
					FakePrimaryAddress: testAddressSample,
					FakeLabels: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "",
						labelGloutonDeny:        "",
						labelGloutonInclDefault: "false",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "",
						labelGloutonDeny:        "",
						labelGloutonInclDefault: "false",
					},
					ExtraLabels: map[string]string{
						types.LabelMetaContainerName:  testContainerMyContainer,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
		{
			name: "k8s-allow-metrics",
			containers: []facts.Container{
				facts.FakeContainer{
					FakeContainerName:  "k8s_containername_podname_namespace",
					FakePodName:        testPodMyPod1234,
					FakePodNamespace:   fakePodNamespace,
					FakePrimaryAddress: testAddressSample,
					FakeAnnotations: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "something,else",
						labelGloutonInclDefault: "1",
					},
				},
			},
			want: []*scrapper.Target{
				{
					URL: mustParse("http://sample:9102/metrics"),
					ContainerLabels: map[string]string{
						labelPromScrape:         labelValueTrue,
						labelGloutonAllow:       "something,else",
						labelGloutonInclDefault: "1",
					},
					ExtraLabels: map[string]string{
						types.LabelK8SNamespace:       fakePodNamespace,
						types.LabelK8SPODName:         testPodMyPod1234,
						types.LabelMetaScrapeJob:      fakeJobName,
						types.LabelMetaScrapeInstance: testInstanceSample9102,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := DynamicScrapper{
				DynamicJobName: "jobname",
			}
			got := d.listExporters(tt.containers)

			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreUnexported(scrapper.Target{})); diff != "" {
				t.Errorf("ListExporters() != want: %v", diff)
			}
		})
	}
}

// allowAllScrapeFilter is a registry metricFilter used by the dynamic scrapper tests: it
// accepts every point/family, so the registered series keep their post-relabel identities.
type allowAllScrapeFilter struct{}

func (allowAllScrapeFilter) FilterPoints(points []types.MetricPoint) []types.MetricPoint {
	return points
}

func (allowAllScrapeFilter) FilterFamilies(families []*dto.MetricFamily) []*dto.MetricFamily {
	return families
}

func (allowAllScrapeFilter) IsMetricAllowed(labels.Labels) bool {
	return true
}

// recordedPusher records every point the registry pushes (values and stale markers).
type recordedPusher struct {
	mu     sync.Mutex
	points []types.MetricPoint
}

func (p *recordedPusher) PushPoints(_ context.Context, points []types.MetricPoint) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.points = append(p.points, points...)
}

func (p *recordedPusher) snapshot() []types.MetricPoint {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]types.MetricPoint, len(p.points))
	copy(out, p.points)

	return out
}

// httpExporter is a controllable /metrics endpoint: body/status can be toggled to simulate
// scrape failures.
type httpExporter struct {
	mu     sync.Mutex
	body   string
	status int
}

func (e *httpExporter) set(body string, status int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.body, e.status = body, status
}

func (e *httpExporter) serveHTTP(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	body, status := e.body, e.status
	e.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func newExporterServer(t *testing.T, body string) (*httptest.Server, *httpExporter) {
	t.Helper()

	exporter := &httpExporter{body: body, status: http.StatusOK}
	server := httptest.NewServer(http.HandlerFunc(exporter.serveHTTP))
	t.Cleanup(server.Close)

	return server, exporter
}

const twoSeriesBody = "# TYPE test_gauge gauge\n" +
	"test_gauge{kind=\"one\"} 1\n" +
	"test_gauge{kind=\"two\"} 2\n"

const oneSeriesBody = "# TYPE test_gauge gauge\n" +
	"test_gauge{kind=\"one\"} 1\n"

const sharedSeriesBody = "# TYPE shared_gauge gauge\n" +
	"shared_gauge 11\n"

func newTestDynamicScrapper(t *testing.T) (*DynamicScrapper, *registry.Registry, *recordedPusher) {
	t.Helper()

	pusher := &recordedPusher{}

	reg, err := registry.New(registry.Option{Filter: allowAllScrapeFilter{}, PushPoint: pusher})
	if err != nil {
		t.Fatal(err)
	}

	d := &DynamicScrapper{
		Registry:       reg,
		DynamicJobName: fakeJobName,
	}

	return d, reg, pusher
}

func exporterContainer(containerName, host string, port int) facts.Container {
	return facts.FakeContainer{
		FakeContainerName:  containerName,
		FakePrimaryAddress: host,
		FakeLabels: map[string]string{
			labelPromScrape: labelValueTrue,
			labelPromPort:   strconv.Itoa(port),
		},
	}
}

func scrapeTargetNow(t *testing.T, d *DynamicScrapper, u string) {
	t.Helper()

	d.l.Lock()
	entry, ok := d.registered[u]
	d.l.Unlock()

	if !ok {
		t.Fatalf("no registration for target %s", u)
	}

	entry.registration.InternalRunScrape(context.Background(), context.Background(), time.Now())
}

func targetURL(server *httptest.Server) string {
	return server.URL + "/metrics"
}

func pointsForInstance(points []types.MetricPoint, scrapeInstance string, stale bool) []types.MetricPoint {
	result := make([]types.MetricPoint, 0)

	for _, p := range points {
		if p.Labels[types.LabelScrapeInstance] != scrapeInstance {
			continue
		}

		if value.IsStaleNaN(p.Value) != stale {
			continue
		}

		result = append(result, p)
	}

	return result
}

func serverInstance(server *httptest.Server) string {
	return server.Listener.Addr().String()
}

func serverPort(t *testing.T, server *httptest.Server) int {
	t.Helper()

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}

	return port
}

// TestDynamicScrapperIncompleteSnapshotRetains verifies that a non-authoritative container
// snapshot keeps a missing exporter registered with its last scraped series, a later complete
// snapshot keeps using the same registration, and only an authoritative snapshot retires the
// exact series with stale markers.
func TestDynamicScrapperIncompleteSnapshotRetains(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	server, _ := newExporterServer(t, twoSeriesBody)
	defer server.Close()

	instance := serverInstance(server)
	container := exporterContainer(testContainerMyContainer, "127.0.0.1", serverPort(t, server))
	u := targetURL(server)

	// First, authoritative snapshot discovers the exporter.
	d.Update([]facts.Container{container}, true, true)

	if stats := d.Stats(); stats.RegisteredTargets != 1 {
		t.Fatalf("stats after discovery = %+v, want 1 registered target", stats)
	}

	scrapeTargetNow(t, d, u)

	if values := pointsForInstance(pusher.snapshot(), instance, false); len(values) != 2 {
		t.Fatalf("got %d value points for %s, want 2", len(values), instance)
	}

	// Partial snapshot: the exporter is missing but must not be forgotten.
	d.Update(nil, false, false)

	stats := d.Stats()
	if stats.RegisteredTargets != 1 || stats.RetainedTargets != 1 || stats.IncompleteSnapshots != 1 || stats.RetiredTargets != 0 {
		t.Fatalf("stats after incomplete snapshot = %+v, want the target retained", stats)
	}

	if stale := pointsForInstance(pusher.snapshot(), instance, true); len(stale) != 0 {
		t.Fatalf("incomplete snapshot emitted %d stale marker(s), want none", len(stale))
	}

	if got := d.GetRegisteredLabels(); got[u][types.LabelMetaScrapeInstance] != instance {
		t.Fatalf("GetRegisteredLabels lost the target after incomplete snapshot: %v", got)
	}

	// Next complete snapshot sees the exporter again: same registration, no counters churn.
	before := d.GetRegisteredLabels()[u]

	d.Update([]facts.Container{container}, true, true)

	after := d.GetRegisteredLabels()[u]
	if cmp.Diff(before, after) != "" {
		t.Errorf("registration labels changed after the confirming snapshot: %s", cmp.Diff(before, after))
	}

	stats = d.Stats()
	if stats.RegisteredTargets != 1 || stats.RetiredTargets != 0 {
		t.Fatalf("stats after confirming snapshot = %+v, want the same single target", stats)
	}

	scrapeTargetNow(t, d, u)

	// Authoritative snapshot confirms the exporter is gone.
	d.Update(nil, true, true)

	stats = d.Stats()
	if stats.RegisteredTargets != 0 || stats.RetiredTargets != 1 {
		t.Fatalf("stats after authoritative removal = %+v, want 0 registered / 1 retired", stats)
	}

	stale := pointsForInstance(pusher.snapshot(), instance, true)
	if len(stale) != 2 {
		t.Fatalf("got %d stale markers for %s, want 2", len(stale), instance)
	}

	kinds := map[string]bool{}
	for _, p := range stale {
		if p.Labels[types.LabelName] != "test_gauge" {
			t.Errorf("stale marker has metric name %q, want test_gauge", p.Labels[types.LabelName])
		}

		kinds[p.Labels["kind"]] = true
	}

	if !kinds["one"] || !kinds["two"] {
		t.Errorf("stale marker kinds = %v, want both one and two", kinds)
	}
}

// TestDynamicScrapperIdentityReplacement verifies that changing the scrape labels (here the
// exporter port for the same container) converges the old identity (stale markers) before
// enabling the new one, and that new samples carry the new instance label.
func TestDynamicScrapperIdentityReplacement(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	serverA, _ := newExporterServer(t, oneSeriesBody)
	defer serverA.Close()

	serverB, _ := newExporterServer(t, twoSeriesBody)
	defer serverB.Close()

	instanceA := serverInstance(serverA)
	instanceB := serverInstance(serverB)
	uA := targetURL(serverA)
	uB := targetURL(serverB)

	containerA := exporterContainer("web", "127.0.0.1", serverPort(t, serverA))

	d.Update([]facts.Container{containerA}, true, true)
	scrapeTargetNow(t, d, uA)

	if values := pointsForInstance(pusher.snapshot(), instanceA, false); len(values) != 1 {
		t.Fatalf("got %d values for old identity %s, want 1", len(values), instanceA)
	}

	// Same container now exposes its exporter on serverB's port: URL and labels change.
	containerB := exporterContainer("web", "127.0.0.1", serverPort(t, serverB))

	d.Update([]facts.Container{containerB}, true, true)

	stats := d.Stats()
	if stats.RegisteredTargets != 1 || stats.RetiredTargets != 1 {
		t.Fatalf("stats after URL replacement = %+v, want 1 target / 1 retirement", stats)
	}

	if _, stillRegistered := d.registered[uA]; stillRegistered {
		t.Errorf("old identity %s is still registered after replacement", uA)
	}

	scrapeTargetNow(t, d, uB)

	staleA := pointsForInstance(pusher.snapshot(), instanceA, true)
	if len(staleA) != 1 {
		t.Fatalf("got %d stale markers for old identity %s, want 1", len(staleA), instanceA)
	}

	if staleB := pointsForInstance(pusher.snapshot(), instanceB, true); len(staleB) != 0 {
		t.Fatalf("new identity %s received %d stale markers, want none", instanceB, len(staleB))
	}

	valuesB := pointsForInstance(pusher.snapshot(), instanceB, false)
	if len(valuesB) != 2 {
		t.Fatalf("got %d value points for new identity %s, want 2", len(valuesB), instanceB)
	}
}

// TestDynamicScrapperSameURLLabelsReplacement verifies the explicit replacement counter for
// a target whose labels change while its URL stays the same (e.g. pod/container rename).
func TestDynamicScrapperSameURLLabelsReplacement(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	server, _ := newExporterServer(t, oneSeriesBody)
	defer server.Close()

	instance := serverInstance(server)
	u := targetURL(server)

	container1 := exporterContainer("web", "127.0.0.1", serverPort(t, server))

	d.Update([]facts.Container{container1}, true, true)
	scrapeTargetNow(t, d, u)

	// Same URL, different container name -> different extra labels at the same URL.
	container2 := exporterContainer("web-renamed", "127.0.0.1", serverPort(t, server))

	d.Update([]facts.Container{container2}, true, false)

	stats := d.Stats()
	if stats.ReplacedIdentities != 1 || stats.RegisteredTargets != 1 {
		t.Fatalf("stats = %+v, want 1 identity replacement / 1 target", stats)
	}

	if stale := pointsForInstance(pusher.snapshot(), instance, true); len(stale) != 1 {
		t.Fatalf("got %d stale markers for replaced same-URL identity %s, want 1", len(stale), instance)
	}

	// The replacement happens even on a non-authoritative snapshot: the new identity is
	// present, so this is not an absence decision.
	scrapeTargetNow(t, d, u)
}

// TestDynamicScrapperFailedScrapeRecovery verifies that a failed scrape keeps ownership of
// the previously scraped series (they still get stale-marked when the container is later
// authoritatively removed), and a successful scrape updates the owned set.
func TestDynamicScrapperFailedScrapeRecovery(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	server, exporter := newExporterServer(t, twoSeriesBody)
	defer server.Close()

	instance := serverInstance(server)
	u := targetURL(server)
	container := exporterContainer(testContainerMyContainer, "127.0.0.1", serverPort(t, server))

	d.Update([]facts.Container{container}, true, true)
	scrapeTargetNow(t, d, u)

	// The exporter starts failing: no new values arrive, but ownership is retained.
	exporter.set("not available", http.StatusServiceUnavailable)
	scrapeTargetNow(t, d, u)

	if values := pointsForInstance(pusher.snapshot(), instance, false); len(values) != 2 {
		t.Fatalf("after failed scrape, got %d value points, still want the original 2", len(values))
	}

	// Authoritative removal despite the failed scrape: stale markers use the last good set.
	d.Update(nil, true, true)

	if stale := pointsForInstance(pusher.snapshot(), instance, true); len(stale) != 2 {
		t.Fatalf("got %d stale markers after failed-scrape removal, want the 2 owned series", len(stale))
	}
}

// TestDynamicScrapperDoesNotRetireOtherRegistration verifies that removing one discovered
// exporter never stale-marks the series of another registration that exposes the same metric
// name under a different instance.
func TestDynamicScrapperDoesNotRetireOtherRegistration(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	serverA, _ := newExporterServer(t, sharedSeriesBody)
	defer serverA.Close()

	serverB, _ := newExporterServer(t, sharedSeriesBody)
	defer serverB.Close()

	instanceA := serverInstance(serverA)
	instanceB := serverInstance(serverB)
	uA := targetURL(serverA)
	uB := targetURL(serverB)

	d.Update(
		[]facts.Container{
			exporterContainer("a", "127.0.0.1", serverPort(t, serverA)),
			exporterContainer("b", "127.0.0.1", serverPort(t, serverB)),
		},
		true, true,
	)
	scrapeTargetNow(t, d, uA)
	scrapeTargetNow(t, d, uB)

	// Only container A disappears; B stays.
	d.Update([]facts.Container{exporterContainer("b", "127.0.0.1", serverPort(t, serverB))}, true, true)

	staleA := pointsForInstance(pusher.snapshot(), instanceA, true)
	if len(staleA) != 1 || staleA[0].Labels[types.LabelName] != "shared_gauge" {
		t.Fatalf("stale for removed exporter = %v, want one shared_gauge stale marker for A", staleA)
	}

	if staleB := pointsForInstance(pusher.snapshot(), instanceB, true); len(staleB) != 0 {
		t.Fatalf("surviving exporter B received %d stale markers, want 0", len(staleB))
	}

	if stats := d.Stats(); stats.RegisteredTargets != 1 {
		t.Fatalf("stats = %+v, want exporter B still registered", stats)
	}

	// B can still be scraped and its value isn't a stale marker.
	scrapeTargetNow(t, d, uB)

	valuesB := pointsForInstance(pusher.snapshot(), instanceB, false)
	if len(valuesB) != 2 {
		t.Fatalf("got %d value points for B, want 2", len(valuesB))
	}
}

// TestDynamicScrapperEmptyByDefault verifies the default no-exporter case never retires
// anything, on either kind of snapshot.
func TestDynamicScrapperEmptyByDefault(t *testing.T) {
	d, _, pusher := newTestDynamicScrapper(t)

	d.Update(nil, true, true)
	d.Update(nil, false, false)

	if stats := d.Stats(); stats.RegisteredTargets != 0 || stats.RetiredTargets != 0 || stats.RetainedTargets != 0 {
		t.Fatalf("stats for the no-exporter case = %+v, want all zeros", stats)
	}

	if len(pusher.snapshot()) != 0 {
		t.Fatalf("points pushed with no exporter: %v", pusher.snapshot())
	}
}

// teePusher mirrors every batch to two pushers, like the agent's teePointPusher fans the
// registry output out to the in-memory store and the embedded TSDB.
type teePusher struct {
	primary   types.PointPusher
	secondary types.PointPusher
}

func (t teePusher) PushPoints(ctx context.Context, points []types.MetricPoint) {
	t.primary.PushPoints(ctx, points)
	t.secondary.PushPoints(ctx, points)
}

// TestDynamicScrapperStaleGenerationReachesMemoryStoreAndTSDB verifies end to end that one
// consistent stale generation from an authoritative retirement removes the series from the
// in-memory store and commits stale markers into the embedded TSDB, so PromQL lookback can't
// resurrect the old values.
func TestDynamicScrapperStaleGenerationReachesMemoryStoreAndTSDB(t *testing.T) {
	memStore := gloutonStore.New("test store", time.Hour, 2*time.Hour)

	localTSDB, err := gloutonTSDB.Open(gloutonTSDB.Options{Path: t.TempDir(), Retention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		localTSDB.Close() //nolint:errcheck
	})

	pusher := teePusher{primary: memStore, secondary: localTSDB}

	reg, err := registry.New(registry.Option{Filter: allowAllScrapeFilter{}, PushPoint: pusher})
	if err != nil {
		t.Fatal(err)
	}

	d := &DynamicScrapper{Registry: reg, DynamicJobName: fakeJobName}

	server, _ := newExporterServer(t, twoSeriesBody)
	defer server.Close()

	instance := serverInstance(server)
	u := targetURL(server)
	container := exporterContainer(testContainerMyContainer, "127.0.0.1", serverPort(t, server))

	d.Update([]facts.Container{container}, true, true)
	scrapeTargetNow(t, d, u)
	// Let the first sample sit strictly before the stale generation.
	time.Sleep(20 * time.Millisecond)

	metrics, _ := memStore.Metrics(map[string]string{types.LabelName: "test_gauge"})
	if len(metrics) != 2 {
		t.Fatalf("memory store has %d test_gauge series after scrape, want 2", len(metrics))
	}

	// Before retirement both series are queryable in the TSDB and end on regular values.
	beforeStale := time.Now()

	if series := queryTSDBTestGauge(t, localTSDB, instance, beforeStale.Add(-time.Hour), beforeStale.Add(time.Minute)); len(series) != 2 {
		t.Fatalf("TSDB returned %d test_gauge series before retirement, want 2", len(series))
	} else {
		for name, lastValue := range series {
			if math.Float64bits(lastValue) == value.StaleNaN {
				t.Errorf("series %s already ends on a stale marker before retirement", name)
			}
		}
	}

	d.Update(nil, true, true)

	metrics, _ = memStore.Metrics(map[string]string{types.LabelName: "test_gauge"})
	if len(metrics) != 0 {
		t.Fatalf("memory store still has %d test_gauge series after authoritative retirement", len(metrics))
	}

	// After the stale generation, the last sample of each series in a window covering both
	// the scrapes and the retirement is the stale marker: this is exactly what PromQL
	// lookback checks, so old values can't be resurrected past the retirement timestamp.
	afterStale := time.Now().Add(time.Minute)

	retired := queryTSDBTestGauge(t, localTSDB, instance, beforeStale.Add(-time.Hour), afterStale)
	if len(retired) != 2 {
		t.Fatalf("TSDB returned %d retired series, want the 2 series each terminated by a stale marker", len(retired))
	}

	for name, lastValue := range retired {
		if !value.IsStaleNaN(lastValue) {
			t.Errorf("last TSDB sample of %s is %v after retirement, want StaleNaN (lookback would otherwise resurrect it)", name, lastValue)
		}
	}
}

// queryTSDBTestGauge returns, per test_gauge series of the given instance, the last sample
// value iterated in the [mint, maxt] window. PromQL lookback treats a final StaleNaN sample
// as the end of the series, so callers can assert a series is alive (regular value) or
// retired (last value is the stale marker).
func queryTSDBTestGauge(t *testing.T, localTSDB *gloutonTSDB.Store, instance string, mint, maxt time.Time) map[string]float64 {
	t.Helper()

	querier, err := localTSDB.Querier(mint.UnixMilli(), maxt.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	// This Prometheus version requires explicit matchers: a nil matcher set selects nothing.
	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, types.LabelName, "test_gauge")
	instanceMatcher := labels.MustNewMatcher(labels.MatchEqual, types.LabelScrapeInstance, instance)

	seriesSet := querier.Select(context.Background(), true, nil, nameMatcher, instanceMatcher)

	result := map[string]float64{}

	for seriesSet.Next() {
		lbls := seriesSet.At().Labels()

		var lastValue float64

		iterator := seriesSet.At().Iterator(nil)

		for valueType := iterator.Next(); valueType != chunkenc.ValNone; valueType = iterator.Next() {
			if valueType != chunkenc.ValFloat {
				t.Fatalf("unexpected value type %v for %s", valueType, lbls)
			}

			_, lastValue = iterator.At()
		}

		result[lbls.String()] = lastValue
	}

	if err := seriesSet.Err(); err != nil {
		t.Fatal(err)
	}

	return result
}
