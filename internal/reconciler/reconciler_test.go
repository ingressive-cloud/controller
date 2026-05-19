package reconciler

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	apiclient "github.com/ingressive-cloud/controller/internal/api"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// testIngressClass is the IngressClass value every fixture uses. Picked
// distinct from "nginx" so cross-class filter tests have a clear "other"
// value to assert against.
const testIngressClass = "ingressive"

// testConnectorID + testConnectorSlug are stamped into every Upstream and the
// allowlist endpoint. They're informational for the assertions; the API fake
// just records them.
const (
	testConnectorID   = "cn-uuid"
	testConnectorSlug = "test-connector"
)

// putSiteCall captures a recorded PutSite invocation. We snapshot the
// SiteConfiguration by value so subsequent in-place mutation by the
// reconciler can't retroactively change what the assertion sees.
type putSiteCall struct {
	Host string
	Cfg  apiclient.SiteConfiguration
}

// fakeSiteAPI implements apiclient.SiteAPI for unit tests. All calls are
// recorded under the mutex; tests read the slices after Reconcile returns.
//
// sites is the simulated server-side state. PutSite writes into it (so
// follow-up GetSite calls observe the previous PUT), DeleteSite removes
// entries, and GetSite returns a deep-copy of the stored config (or
// (nil, nil) for an unknown host, matching the production API behavior).
// Tests that exercise the cooperative-merge update path can pre-seed `sites`
// to install a "user-edited" baseline.
type fakeSiteAPI struct {
	mu             sync.Mutex
	PutSiteCalls   []putSiteCall
	GetSiteCalls   []string
	DeleteCalls    []string
	AllowlistCalls [][]string

	// sites is the simulated current state, indexed by host.
	sites map[string]apiclient.SiteConfiguration

	// FailPutSite, if non-nil, is returned from every PutSite call.
	FailPutSite error
}

func (f *fakeSiteAPI) GetSite(_ context.Context, host string) (*apiclient.SiteConfiguration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.GetSiteCalls = append(f.GetSiteCalls, host)
	cfg, ok := f.sites[host]
	if !ok {
		return nil, nil
	}
	return cloneSiteConfig(&cfg), nil
}

func (f *fakeSiteAPI) PutSite(_ context.Context, host string, cfg apiclient.SiteConfiguration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailPutSite != nil {
		return f.FailPutSite
	}
	// Defensive deep-copy of the location slice: the reconciler builds the
	// merged SiteConfiguration by appending to a backing array shared across
	// hosts in some edge cases. Snapshotting here keeps test assertions
	// independent of subsequent reconciler mutations.
	cfgCopy := cfg
	if len(cfg.Locations) > 0 {
		locs := make([]apiclient.SiteLocation, len(cfg.Locations))
		copy(locs, cfg.Locations)
		cfgCopy.Locations = locs
	}
	f.PutSiteCalls = append(f.PutSiteCalls, putSiteCall{Host: host, Cfg: cfgCopy})
	// Mirror the server: subsequent GetSite for this host returns what we
	// just PUT, so a second reconcile pass exercises the update branch.
	if f.sites == nil {
		f.sites = make(map[string]apiclient.SiteConfiguration)
	}
	f.sites[host] = cfgCopy
	return nil
}

func (f *fakeSiteAPI) DeleteSite(_ context.Context, host string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DeleteCalls = append(f.DeleteCalls, host)
	delete(f.sites, host)
	return nil
}

func (f *fakeSiteAPI) PutConnectorServices(_ context.Context, _ string, urls []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := append([]string(nil), urls...)
	f.AllowlistCalls = append(f.AllowlistCalls, snapshot)
	return nil
}

// snapshot helpers, used to read the recorded calls while holding the lock.
func (f *fakeSiteAPI) putSites() []putSiteCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]putSiteCall(nil), f.PutSiteCalls...)
}
func (f *fakeSiteAPI) gets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.GetSiteCalls...)
}
func (f *fakeSiteAPI) deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.DeleteCalls...)
}
// seedSite installs a pre-existing SiteConfiguration the next GetSite call
// will return. Used by merge-update tests to set up the "controller has
// already applied something here" baseline.
func (f *fakeSiteAPI) seedSite(host string, cfg apiclient.SiteConfiguration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sites == nil {
		f.sites = make(map[string]apiclient.SiteConfiguration)
	}
	// Snapshot to prevent the test from observing later mutations through
	// its own reference.
	cfgCopy := *cloneSiteConfig(&cfg)
	f.sites[host] = cfgCopy
}
func (f *fakeSiteAPI) allowlists() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.AllowlistCalls))
	for i, c := range f.AllowlistCalls {
		out[i] = append([]string(nil), c...)
	}
	return out
}

// reconcilerScheme is the runtime.Scheme the fake client uses. The standard
// client-go scheme already covers core/v1 + networking/v1, which is everything
// we need.
func reconcilerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	return s
}

// newReconcilerForTest wires up an IngressReconciler with a fake client and
// the supplied seed objects. Returns the reconciler, fake K8s client, and the
// fake API for assertions.
func newReconcilerForTest(t *testing.T, initObjs ...client.Object) (*IngressReconciler, client.Client, *fakeSiteAPI) {
	t.Helper()
	kc := fake.NewClientBuilder().
		WithScheme(reconcilerScheme(t)).
		WithObjects(initObjs...).
		WithStatusSubresource(&networkingv1.Ingress{}).
		Build()
	api := &fakeSiteAPI{}
	r := &IngressReconciler{
		Client:        kc,
		APIClient:     api,
		IngressClass:  testIngressClass,
		ConnectorID:   testConnectorID,
		ConnectorSlug: testConnectorSlug,
		Log:           logr.Discard(),
	}
	return r, kc, api
}

// reconcileUntilStable invokes Reconcile up to maxPasses times for the given
// key. Many of our reconciles split work across two passes (add-finalizer
// then translate), so a single call isn't enough to reach the steady state.
// Returns the number of passes actually performed.
func reconcileUntilStable(t *testing.T, r *IngressReconciler, key types.NamespacedName, maxPasses int) int {
	t.Helper()
	for i := 1; i <= maxPasses; i++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("reconcile pass %d: %v", i, err)
		}
	}
	return maxPasses
}

// ingressForTest builds a minimal Ingress with the supplied class and rules.
// className=="" leaves spec.ingressClassName nil; otherwise we set it.
func ingressForTest(namespace, name, className string, rules ...networkingv1.IngressRule) *networkingv1.Ingress {
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       networkingv1.IngressSpec{Rules: rules},
	}
	if className != "" {
		cn := className
		ing.Spec.IngressClassName = &cn
	}
	return ing
}

// ingressForTestAt is like ingressForTest but stamps an explicit
// CreationTimestamp so the conflict-resolution tests can establish a
// deterministic "oldest" ordering without depending on real-time semantics.
func ingressForTestAt(namespace, name, className string, created time.Time, rules ...networkingv1.IngressRule) *networkingv1.Ingress {
	ing := ingressForTest(namespace, name, className, rules...)
	ing.CreationTimestamp = metav1.NewTime(created)
	return ing
}

// ruleFor builds a single HTTP rule with one prefix path pointing at the
// supplied service:port.
func ruleFor(host, path, serviceName string, servicePort int32) networkingv1.IngressRule {
	pt := networkingv1.PathTypePrefix
	return networkingv1.IngressRule{
		Host: host,
		IngressRuleValue: networkingv1.IngressRuleValue{
			HTTP: &networkingv1.HTTPIngressRuleValue{
				Paths: []networkingv1.HTTPIngressPath{{
					Path:     path,
					PathType: &pt,
					Backend: networkingv1.IngressBackend{
						Service: &networkingv1.IngressServiceBackend{
							Name: serviceName,
							Port: networkingv1.ServiceBackendPort{Number: servicePort},
						},
					},
				}},
			},
		},
	}
}

// serviceFor builds a corev1.Service with one TCP port.
func serviceFor(namespace, name string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: "http", Port: port}},
		},
	}
}

// TestReconcile_NewIngressCreatesSite — happy path. Fresh Ingress with our
// class and a matching Service: PutSite is called, finalizer is added, the
// allowlist gets the URL, status hostname is set, and lastSites tracks the
// host.
func TestReconcile_NewIngressCreatesSite(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "backend", 80))
	svc := serviceFor("default", "backend", 80)
	r, kc, api := newReconcilerForTest(t, ing, svc)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	// Pass 1 adds the finalizer + requeues; pass 2 translates + applies.
	reconcileUntilStable(t, r, key, 2)

	puts := api.putSites()
	if len(puts) != 1 {
		t.Fatalf("expected 1 PutSite call, got %d (%+v)", len(puts), puts)
	}
	if puts[0].Host != "app.example.com" {
		t.Errorf("PutSite host = %q", puts[0].Host)
	}
	if len(puts[0].Cfg.Locations) != 1 || puts[0].Cfg.Locations[0].Upstream.ConnectorID != testConnectorID {
		t.Errorf("SiteConfiguration unexpected: %+v", puts[0].Cfg)
	}

	var got networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get ingress: %v", err)
	}
	if !containsString(got.Finalizers, FinalizerName) {
		t.Errorf("finalizer missing: %v", got.Finalizers)
	}
	if len(got.Status.LoadBalancer.Ingress) != 1 || got.Status.LoadBalancer.Ingress[0].Hostname != "app.example.com" {
		t.Errorf("status not set: %+v", got.Status.LoadBalancer.Ingress)
	}

	als := api.allowlists()
	if len(als) == 0 {
		t.Fatal("no PutConnectorServices calls recorded")
	}
	last := als[len(als)-1]
	if len(last) != 1 || last[0] != "http://backend.default.svc.cluster.local:80" {
		t.Errorf("last allowlist = %v", last)
	}

	// Site was actually put — host should appear in PutSite calls.
	putCalls := api.putSites()
	if len(putCalls) == 0 || putCalls[len(putCalls)-1].Host != "app.example.com" {
		t.Errorf("expected PutSite for app.example.com, got %v", putCalls)
	}
}

// TestReconcile_DifferentClass — Ingress with a different class: no API calls,
// no finalizer.
func TestReconcile_DifferentClass(t *testing.T) {
	ing := ingressForTest("default", "demo", "nginx",
		ruleFor("app.example.com", "/", "backend", 80))
	r, kc, api := newReconcilerForTest(t, ing)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(api.putSites()) != 0 {
		t.Errorf("unexpected PutSite calls for non-matching class: %+v", api.putSites())
	}
	// Note: under the fully-declarative model an empty desired state still
	// drives a single PutConnectorServices call (with `[]`) on every
	// reconcile. That's intentional and we don't assert against it; the
	// receiver collapses identical writes server-side.
	var got networkingv1.Ingress
	_ = kc.Get(context.Background(), key, &got)
	if containsString(got.Finalizers, FinalizerName) {
		t.Errorf("finalizer should not be added for non-matching class")
	}
}

// TestReconcile_NoClass — empty ingressClassName: same outcome as a non-
// matching class.
func TestReconcile_NoClass(t *testing.T) {
	ing := ingressForTest("default", "demo", "",
		ruleFor("app.example.com", "/", "backend", 80))
	r, kc, api := newReconcilerForTest(t, ing)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(api.putSites()) != 0 {
		t.Errorf("unexpected PutSite for nil class")
	}
	var got networkingv1.Ingress
	_ = kc.Get(context.Background(), key, &got)
	if containsString(got.Finalizers, FinalizerName) {
		t.Errorf("finalizer should not be added")
	}
}

// TestReconcile_Update — same Ingress, new path: second PutSite reflects the
// new configuration.
func TestReconcile_Update(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "backend", 80))
	svc := serviceFor("default", "backend", 80)
	r, kc, api := newReconcilerForTest(t, ing, svc)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	reconcileUntilStable(t, r, key, 2)

	// Mutate the Ingress: change the path.
	var live networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &live); err != nil {
		t.Fatalf("get: %v", err)
	}
	live.Spec.Rules[0].HTTP.Paths[0].Path = "/api"
	if err := kc.Update(context.Background(), &live); err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile after update: %v", err)
	}
	puts := api.putSites()
	if len(puts) < 2 {
		t.Fatalf("expected at least 2 PutSite calls after update, got %d", len(puts))
	}
	last := puts[len(puts)-1]
	if last.Cfg.Locations[0].Path != "/api" {
		t.Errorf("last PutSite path = %q, want /api", last.Cfg.Locations[0].Path)
	}
}

// TestReconcile_DeleteWithFinalizer — finalizer present + DeletionTimestamp
// set: DeleteSite + allowlist update + finalizer removal.
func TestReconcile_DeleteWithFinalizer(t *testing.T) {
	now := metav1.Now()
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "backend", 80))
	ing.Finalizers = []string{FinalizerName}
	ing.DeletionTimestamp = &now
	svc := serviceFor("default", "backend", 80)
	r, kc, api := newReconcilerForTest(t, ing, svc)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	// No in-memory seed needed — claimedHosts derives previously-claimed from
	// the (deleting) Ingress's spec.rules in the cluster.

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Under the declarative model, a deleting Ingress is still listed by
	// listManagedIngresses; the recompute will translate + accept it again,
	// keeping the Site alive. That's not what we want for a delete flow.
	// In practice, kubectl shows the object as Terminating; we look at
	// DeletionTimestamp during the recompute and treat it as "not a
	// contributor" so its hosts vanish from desired.
	//
	// Verify that: host should be deleted via DeleteSite + allowlist
	// shrinks to empty + finalizer is removed.
	dels := api.deletes()
	if len(dels) != 1 || dels[0] != "app.example.com" {
		t.Errorf("expected one DeleteSite for app.example.com, got %v", dels)
	}
	als := api.allowlists()
	if len(als) == 0 || len(als[len(als)-1]) != 0 {
		t.Errorf("expected empty allowlist after delete, got %v", als)
	}
	// fake client should have removed our finalizer; the absence of any
	// finalizer means the deletion can complete.
	var got networkingv1.Ingress
	err := kc.Get(context.Background(), key, &got)
	if err == nil && containsString(got.Finalizers, FinalizerName) {
		t.Errorf("finalizer should be removed after delete reconcile")
	}
}

// TestReconcile_MultipleHostsInOneIngress — two rules with different hosts in
// one Ingress: two PutSite calls, status has two entries, union covers both
// upstream URLs.
func TestReconcile_MultipleHostsInOneIngress(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("a.example.com", "/", "svc-a", 80),
		ruleFor("b.example.com", "/", "svc-b", 8080),
	)
	r, kc, api := newReconcilerForTest(t,
		ing,
		serviceFor("default", "svc-a", 80),
		serviceFor("default", "svc-b", 8080),
	)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	reconcileUntilStable(t, r, key, 2)

	puts := api.putSites()
	if len(puts) != 2 {
		t.Fatalf("expected 2 PutSite calls, got %d", len(puts))
	}
	hosts := []string{puts[0].Host, puts[1].Host}
	sort.Strings(hosts)
	if hosts[0] != "a.example.com" || hosts[1] != "b.example.com" {
		t.Errorf("PutSite hosts = %v", hosts)
	}

	var got networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Status.LoadBalancer.Ingress) != 2 {
		t.Errorf("status should have 2 entries, got %d (%+v)", len(got.Status.LoadBalancer.Ingress), got.Status.LoadBalancer.Ingress)
	}

	als := api.allowlists()
	last := als[len(als)-1]
	if len(last) != 2 {
		t.Errorf("allowlist should contain 2 URLs, got %v", last)
	}
}

// TestReconcile_ServiceMissing — backend Service doesn't exist: no PutSite,
// no error. Service watcher will retrigger the reconciler later.
func TestReconcile_ServiceMissing(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "nope", 80))
	r, _, api := newReconcilerForTest(t, ing)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	// Run a couple of passes; we want to ensure even after the finalizer is
	// in place the missing-service branch swallows the soft error.
	reconcileUntilStable(t, r, key, 3)

	if len(api.putSites()) != 0 {
		t.Errorf("PutSite should not be called when Service is missing, got %+v", api.putSites())
	}
}

// TestReconcile_MergedAllowlistUnion — two distinct Ingresses contributing
// different upstream URLs. After a single reconcile the allowlist contains
// the union, regardless of which Ingress triggered the reconcile.
func TestReconcile_MergedAllowlistUnion(t *testing.T) {
	ingA := ingressForTest("default", "demo-a", testIngressClass,
		ruleFor("a.example.com", "/", "svc-a", 80))
	ingB := ingressForTest("default", "demo-b", testIngressClass,
		ruleFor("b.example.com", "/", "svc-b", 80))
	svcA := serviceFor("default", "svc-a", 80)
	svcB := serviceFor("default", "svc-b", 80)
	r, _, api := newReconcilerForTest(t, ingA, ingB, svcA, svcB)

	keyA := types.NamespacedName{Namespace: "default", Name: "demo-a"}
	keyB := types.NamespacedName{Namespace: "default", Name: "demo-b"}
	// Both have to walk through "add finalizer; requeue".
	reconcileUntilStable(t, r, keyA, 2)
	reconcileUntilStable(t, r, keyB, 2)

	als := api.allowlists()
	if len(als) == 0 {
		t.Fatal("no allowlist calls recorded")
	}
	last := als[len(als)-1]
	wantA := "http://svc-a.default.svc.cluster.local:80"
	wantB := "http://svc-b.default.svc.cluster.local:80"
	if !containsString(last, wantA) || !containsString(last, wantB) {
		t.Errorf("final allowlist missing one of the URLs: %v", last)
	}
}

// TestReconcile_StatusUnchangedSkipsUpdate — second reconcile of an Ingress
// already in the steady state must not bump the Ingress's ResourceVersion via
// a status write.
func TestReconcile_StatusUnchangedSkipsUpdate(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "backend", 80))
	svc := serviceFor("default", "backend", 80)
	r, kc, _ := newReconcilerForTest(t, ing, svc)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}

	// Drive it to steady state.
	reconcileUntilStable(t, r, key, 2)

	var before networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &before); err != nil {
		t.Fatalf("get: %v", err)
	}

	// Run one more reconcile; nothing should change.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("idle reconcile: %v", err)
	}

	var after networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &after); err != nil {
		t.Fatalf("get: %v", err)
	}
	if before.ResourceVersion != after.ResourceVersion {
		t.Errorf("idle reconcile rewrote Ingress (rv %q -> %q)", before.ResourceVersion, after.ResourceVersion)
	}
}

// TestReconcile_StrayNotFound — Reconcile fires for an Ingress that doesn't
// exist (race between watch event and processing). We still do the global
// recompute; with no managed Ingresses, the allowlist should be empty and no
// DeleteSite is fired (nothing was previously claimed in cluster state).
func TestReconcile_StrayNotFound(t *testing.T) {
	r, _, api := newReconcilerForTest(t /* no objects */)
	key := types.NamespacedName{Namespace: "default", Name: "gone"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	als := api.allowlists()
	if len(als) == 0 || len(als[len(als)-1]) != 0 {
		t.Errorf("expected empty allowlist after stray notfound, got %v", als)
	}
	if dels := api.deletes(); len(dels) != 0 {
		t.Errorf("no DeleteSite expected (no cluster state to derive from), got %v", dels)
	}
	// Sanity: the API responded that the object is not found (we never created it).
	var ing networkingv1.Ingress
	err := r.Get(context.Background(), key, &ing)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound, got %v", err)
	}
}

// TestReconcile_DeletingIngressClearsSiteAcrossRestarts — the bug fix in
// action: an Ingress with our finalizer + DeletionTimestamp set must have its
// Site DELETEd even when the reconciler's in-memory state is empty (mimicking
// a controller pod restart between Site creation and Ingress deletion).
//
// The previous implementation tracked sites via an in-memory lastSites map.
// On restart that map was empty, so the delete-cleanup diff produced no
// DeleteSite calls and the Site was orphaned in Bifrost. The cluster-derived
// claimedHosts approach reads the deleting Ingress's spec.rules directly and
// produces the correct DELETE.
func TestReconcile_DeletingIngressClearsSiteAcrossRestarts(t *testing.T) {
	t0 := time.Now()
	ing := ingressForTestAt("default", "hello", testIngressClass, t0,
		ruleFor("hello.example.com", "/", "hello", 80))
	// Simulate "we managed this before but the pod restarted": finalizer
	// already present, DeletionTimestamp set, but no in-memory state.
	now := metav1.NewTime(t0.Add(time.Minute))
	ing.DeletionTimestamp = &now
	controllerutil.AddFinalizer(ing, FinalizerName)
	ing.Finalizers = append([]string(nil), ing.Finalizers...) // ensure slice owned

	r, _, api := newReconcilerForTest(t, ing,
		serviceFor("default", "hello", 80),
	)
	key := types.NamespacedName{Namespace: "default", Name: "hello"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	dels := api.deletes()
	if len(dels) != 1 || dels[0] != "hello.example.com" {
		t.Errorf("expected DeleteSite for hello.example.com, got %v", dels)
	}
}

// TestReconcile_TwoIngressesSameHost_DifferentPaths — two Ingresses contribute
// non-conflicting paths to the same host. The merged SiteConfiguration must
// include locations from both.
func TestReconcile_TwoIngressesSameHost_DifferentPaths(t *testing.T) {
	t0 := time.Now()
	ingA := ingressForTestAt("default", "team-api", testIngressClass, t0,
		ruleFor("shared.example.com", "/api", "svc-api", 80))
	ingB := ingressForTestAt("default", "team-web", testIngressClass, t0.Add(time.Second),
		ruleFor("shared.example.com", "/web", "svc-web", 80))
	r, _, api := newReconcilerForTest(t,
		ingA, ingB,
		serviceFor("default", "svc-api", 80),
		serviceFor("default", "svc-web", 80),
	)
	keyA := types.NamespacedName{Namespace: "default", Name: "team-api"}
	keyB := types.NamespacedName{Namespace: "default", Name: "team-web"}
	reconcileUntilStable(t, r, keyA, 2)
	reconcileUntilStable(t, r, keyB, 2)

	puts := api.putSites()
	if len(puts) == 0 {
		t.Fatal("no PutSite calls recorded")
	}
	// Look at the most recent PutSite for our shared host.
	var last *putSiteCall
	for i := range puts {
		p := puts[i]
		if p.Host == "shared.example.com" {
			last = &puts[i]
		}
	}
	if last == nil {
		t.Fatalf("no PutSite for shared host in %+v", puts)
	}
	if len(last.Cfg.Locations) != 2 {
		t.Fatalf("expected 2 locations in merged config, got %d (%+v)", len(last.Cfg.Locations), last.Cfg.Locations)
	}
	paths := []string{last.Cfg.Locations[0].Path, last.Cfg.Locations[1].Path}
	sort.Strings(paths)
	if paths[0] != "/api" || paths[1] != "/web" {
		t.Errorf("merged locations missing expected paths: %v", paths)
	}

	// And the allowlist union has both services.
	last2 := api.allowlists()[len(api.allowlists())-1]
	if !containsString(last2, "http://svc-api.default.svc.cluster.local:80") ||
		!containsString(last2, "http://svc-web.default.svc.cluster.local:80") {
		t.Errorf("allowlist missing one of the URLs: %v", last2)
	}
}

// TestReconcile_PathConflict_OlderWins — both Ingresses target the same
// (host, path). The older Ingress (lower CreationTimestamp) wins and the
// newer one's contribution is dropped.
func TestReconcile_PathConflict_OlderWins(t *testing.T) {
	t0 := time.Now()
	older := ingressForTestAt("default", "older", testIngressClass, t0,
		ruleFor("shared.example.com", "/api", "svc-old", 80))
	newer := ingressForTestAt("default", "newer", testIngressClass, t0.Add(time.Second),
		ruleFor("shared.example.com", "/api", "svc-new", 80))
	r, kc, api := newReconcilerForTest(t,
		older, newer,
		serviceFor("default", "svc-old", 80),
		serviceFor("default", "svc-new", 80),
	)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "older"}, 2)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "newer"}, 2)

	puts := api.putSites()
	// Every PutSite for the shared host should point at svc-old, never
	// svc-new.
	for _, p := range puts {
		if p.Host != "shared.example.com" {
			continue
		}
		for _, loc := range p.Cfg.Locations {
			if loc.Upstream.Service == "http://svc-new.default.svc.cluster.local:80" {
				t.Errorf("newer Ingress's upstream leaked into PutSite: %+v", loc)
			}
		}
	}

	// Allowlist union must NOT include the rejected Ingress's service.
	last := api.allowlists()[len(api.allowlists())-1]
	if containsString(last, "http://svc-new.default.svc.cluster.local:80") {
		t.Errorf("rejected Ingress's URL must not appear in allowlist: %v", last)
	}
	if !containsString(last, "http://svc-old.default.svc.cluster.local:80") {
		t.Errorf("older Ingress's URL missing from allowlist: %v", last)
	}

	// The rejected (newer) Ingress's status should be cleared/empty.
	var newerIng networkingv1.Ingress
	if err := kc.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "newer"}, &newerIng); err != nil {
		t.Fatalf("get newer: %v", err)
	}
	if len(newerIng.Status.LoadBalancer.Ingress) != 0 {
		t.Errorf("rejected Ingress should have empty status, got %+v", newerIng.Status.LoadBalancer.Ingress)
	}
}

// TestReconcile_PathConflict_RejectsEntireIngress — a newer Ingress contributes
// two hosts: one conflicts on a path, one is otherwise uncontested. The whole
// Ingress is rejected, so the uncontested host gets no PutSite either.
func TestReconcile_PathConflict_RejectsEntireIngress(t *testing.T) {
	t0 := time.Now()
	older := ingressForTestAt("default", "older", testIngressClass, t0,
		ruleFor("shared.example.com", "/api", "svc-old", 80))
	newer := ingressForTestAt("default", "newer", testIngressClass, t0.Add(time.Second),
		ruleFor("shared.example.com", "/api", "svc-new", 80),
		ruleFor("solo.example.com", "/", "svc-solo", 80),
	)
	r, _, api := newReconcilerForTest(t,
		older, newer,
		serviceFor("default", "svc-old", 80),
		serviceFor("default", "svc-new", 80),
		serviceFor("default", "svc-solo", 80),
	)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "older"}, 2)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "newer"}, 2)

	puts := api.putSites()
	// No PutSite should ever have been emitted for solo.example.com — the
	// rejection of the newer Ingress is whole-Ingress.
	for _, p := range puts {
		if p.Host == "solo.example.com" {
			t.Errorf("solo.example.com must not be PutSite'd while the contributing Ingress is rejected: %+v", p)
		}
	}
	// Allowlist must not contain svc-solo either.
	last := api.allowlists()[len(api.allowlists())-1]
	if containsString(last, "http://svc-solo.default.svc.cluster.local:80") {
		t.Errorf("svc-solo must not appear in allowlist; the Ingress contributing it is rejected: %v", last)
	}
}

// TestReconcile_PathConflict_ResolvedByDeletingWinner — after the older
// Ingress disappears the newer one becomes the sole claimant for the path
// and its Site materialises.
func TestReconcile_PathConflict_ResolvedByDeletingWinner(t *testing.T) {
	t0 := time.Now()
	older := ingressForTestAt("default", "older", testIngressClass, t0,
		ruleFor("shared.example.com", "/api", "svc-old", 80))
	newer := ingressForTestAt("default", "newer", testIngressClass, t0.Add(time.Second),
		ruleFor("shared.example.com", "/api", "svc-new", 80))
	r, kc, api := newReconcilerForTest(t,
		older, newer,
		serviceFor("default", "svc-old", 80),
		serviceFor("default", "svc-new", 80),
	)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "older"}, 2)
	reconcileUntilStable(t, r, types.NamespacedName{Namespace: "default", Name: "newer"}, 2)

	// Confirm the rejected newer Ingress's URL is NOT in the current
	// allowlist before we touch the older one.
	before := api.allowlists()[len(api.allowlists())-1]
	if containsString(before, "http://svc-new.default.svc.cluster.local:80") {
		t.Fatalf("precondition: rejected URL must not be in allowlist yet: %v", before)
	}

	// Now hard-delete the older Ingress (no finalizer present in the fake;
	// the seed object had none, so Delete completes immediately).
	if err := kc.Delete(context.Background(), older); err != nil {
		t.Fatalf("delete older: %v", err)
	}

	// Reconcile against the newer Ingress's key. The recompute should
	// observe that older is gone, promote newer to accepted, and PutSite
	// the shared host with newer's upstream.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "newer"},
	}); err != nil {
		t.Fatalf("reconcile after winner delete: %v", err)
	}

	// Find the most recent PutSite for shared.example.com.
	puts := api.putSites()
	var latest *putSiteCall
	for i := range puts {
		if puts[i].Host == "shared.example.com" {
			latest = &puts[i]
		}
	}
	if latest == nil {
		t.Fatalf("no PutSite for shared host after winner deletion: %+v", puts)
	}
	if len(latest.Cfg.Locations) != 1 ||
		latest.Cfg.Locations[0].Upstream.Service != "http://svc-new.default.svc.cluster.local:80" {
		t.Errorf("expected newer's upstream in latest PutSite, got %+v", latest.Cfg.Locations)
	}

	// Allowlist now includes svc-new and not svc-old.
	last := api.allowlists()[len(api.allowlists())-1]
	if !containsString(last, "http://svc-new.default.svc.cluster.local:80") {
		t.Errorf("svc-new should be in allowlist after winner delete: %v", last)
	}
	if containsString(last, "http://svc-old.default.svc.cluster.local:80") {
		t.Errorf("svc-old should NOT be in allowlist after winner delete: %v", last)
	}
}

// TestReconcile_HostRemovedFromIngress_KnownLeak documents a v1 limitation:
// if a user edits an Ingress to drop a host (without deleting the whole
// Ingress), the dropped host's Site is leaked. claimedHosts derives the
// previously-claimed set from current cluster state — once the spec.rules
// entry is gone, we can't tell we used to manage that host.
//
// The fix is annotation-based tracking on each Ingress recording the hosts
// it successfully contributed at the previous reconcile. Not implemented for
// v1. When it lands, flip this test to assert the host IS cleaned up.
func TestReconcile_HostRemovedFromIngress_KnownLeak(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("a.example.com", "/", "svc-a", 80),
		ruleFor("b.example.com", "/", "svc-b", 80),
	)
	r, kc, api := newReconcilerForTest(t,
		ing,
		serviceFor("default", "svc-a", 80),
		serviceFor("default", "svc-b", 80),
	)
	key := types.NamespacedName{Namespace: "default", Name: "demo"}
	reconcileUntilStable(t, r, key, 2)

	// Drop b.example.com from spec.
	var live networkingv1.Ingress
	if err := kc.Get(context.Background(), key, &live); err != nil {
		t.Fatalf("get: %v", err)
	}
	live.Spec.Rules = live.Spec.Rules[:1] // keep only "a"
	if err := kc.Update(context.Background(), &live); err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile after host drop: %v", err)
	}

	// EXPECTED v1 BEHAVIOR: no DeleteSite for b.example.com — we don't know
	// it was previously claimed. When annotation tracking lands, this should
	// become `len(dels) == 1 && dels[0] == "b.example.com"`.
	dels := api.deletes()
	if len(dels) != 0 {
		t.Errorf("v1 limitation: expected NO DeleteSite (host-drop leak); got %v", dels)
	}
}

// TestReconcile_ComputeDesired_Direct exercises computeDesired in isolation
// so we can assert on its returned structures without going through the full
// Reconcile API-call dance.
func TestReconcile_ComputeDesired_Direct(t *testing.T) {
	t0 := time.Now()
	older := ingressForTestAt("default", "older", testIngressClass, t0,
		ruleFor("shared.example.com", "/api", "svc-old", 80))
	newer := ingressForTestAt("default", "newer", testIngressClass, t0.Add(time.Second),
		ruleFor("shared.example.com", "/api", "svc-new", 80))
	missing := ingressForTestAt("default", "missing", testIngressClass, t0.Add(2*time.Second),
		ruleFor("alone.example.com", "/", "ghost", 80))

	r, _, _ := newReconcilerForTest(t,
		older, newer, missing,
		serviceFor("default", "svc-old", 80),
		serviceFor("default", "svc-new", 80),
	)

	merged, rejected, skipped := r.computeDesired(context.Background(), []networkingv1.Ingress{*newer, *older, *missing})
	if _, ok := merged["shared.example.com"]; !ok {
		t.Errorf("merged should include shared host, got %v", merged)
	}
	if got := len(rejected); got != 1 {
		t.Fatalf("expected 1 rejection (newer), got %d (%+v)", got, rejected)
	}
	if rejected[0].Ingress.Name != "newer" {
		t.Errorf("expected newer to be rejected, got %v", rejected[0])
	}
	if rejected[0].WinnerKey.Name != "older" {
		t.Errorf("expected older to be winner, got %v", rejected[0])
	}
	if got := len(skipped); got != 1 {
		t.Fatalf("expected 1 skip (missing service), got %d (%+v)", got, skipped)
	}
	if skipped[0].Ingress.Name != "missing" {
		t.Errorf("expected `missing` to be skipped, got %v", skipped[0])
	}
}

// TestReconcile_CooperativeMerge_PreservesUserEdit — a real Ingress exists in
// the cluster + Bifrost already has a Site for it (seeded into the fake) with
// a user-edited client_max_body_size. The reconciler must GET, merge, and
// PUT a config that keeps the user's "50m" value, even though the translator
// produces ClientMaxBodySize == "" (no annotation on the Ingress).
func TestReconcile_CooperativeMerge_PreservesUserEdit(t *testing.T) {
	ing := ingressForTest("default", "demo", testIngressClass,
		ruleFor("app.example.com", "/", "backend", 80))
	svc := serviceFor("default", "backend", 80)
	r, _, api := newReconcilerForTest(t, ing, svc)

	// Seed a "user-edited" current Site: the controller previously PUT a
	// location at "/", the user then bumped client_max_body_size in the
	// console.
	api.seedSite("app.example.com", apiclient.SiteConfiguration{
		Locations: []apiclient.SiteLocation{{
			LocationType: "prefix",
			Path:         "/",
			Upstream: apiclient.LocationUpstream{
				ConnectorID: testConnectorID,
				Service:     "http://backend.default.svc.cluster.local:80",
			},
			ClientMaxBodySize: "50m",
		}},
	})

	key := types.NamespacedName{Namespace: "default", Name: "demo"}
	reconcileUntilStable(t, r, key, 2)

	puts := api.putSites()
	// Existing seed already matches what the translator produces in routing
	// fields, and the user-edited "50m" is preserved by the merge — so the
	// merged config equals current and PutSite must NOT fire.
	for _, p := range puts {
		if p.Host == "app.example.com" {
			t.Errorf("expected no PutSite for unchanged merged config (user edit preserved), got %+v", p.Cfg)
		}
	}

	// And GetSite was called — the cooperative path is exercised.
	if !containsString(api.gets(), "app.example.com") {
		t.Errorf("expected GetSite for app.example.com, got %v", api.gets())
	}
}

// TestReconcile_CooperativeMerge_TranslatorChangePersisted — the user has
// edited a Site in the console (e.g. set shield_id). The Ingress is then
// modified to add a new path. The merge must PUT a config that includes the
// new path AND preserves the user's shield_id on the existing location.
func TestReconcile_CooperativeMerge_TranslatorChangePersisted(t *testing.T) {
	pt := networkingv1.PathTypePrefix
	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "demo"},
		Spec: networkingv1.IngressSpec{
			IngressClassName: func() *string { s := testIngressClass; return &s }(),
			Rules: []networkingv1.IngressRule{{
				Host: "app.example.com",
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{
							{
								Path: "/", PathType: &pt,
								Backend: networkingv1.IngressBackend{
									Service: &networkingv1.IngressServiceBackend{
										Name: "backend", Port: networkingv1.ServiceBackendPort{Number: 80},
									},
								},
							},
							{
								Path: "/v2", PathType: &pt,
								Backend: networkingv1.IngressBackend{
									Service: &networkingv1.IngressServiceBackend{
										Name: "backend", Port: networkingv1.ServiceBackendPort{Number: 80},
									},
								},
							},
						},
					},
				},
			}},
		},
	}
	svc := serviceFor("default", "backend", 80)
	r, _, api := newReconcilerForTest(t, ing, svc)

	// Seed: controller previously PUT just "/" — the user added a shield_id.
	api.seedSite("app.example.com", apiclient.SiteConfiguration{
		Locations: []apiclient.SiteLocation{{
			LocationType: "prefix",
			Path:         "/",
			Upstream: apiclient.LocationUpstream{
				ConnectorID: testConnectorID,
				Service:     "http://backend.default.svc.cluster.local:80",
			},
			ShieldID: "shield-abc",
		}},
	})

	key := types.NamespacedName{Namespace: "default", Name: "demo"}
	reconcileUntilStable(t, r, key, 2)

	puts := api.putSites()
	if len(puts) == 0 {
		t.Fatal("expected at least one PutSite for the merged update")
	}
	last := puts[len(puts)-1]
	if last.Host != "app.example.com" {
		t.Fatalf("last PutSite host = %q", last.Host)
	}
	if len(last.Cfg.Locations) != 2 {
		t.Fatalf("expected 2 locations after merge, got %d (%+v)", len(last.Cfg.Locations), last.Cfg.Locations)
	}
	var rootLoc *apiclient.SiteLocation
	for i := range last.Cfg.Locations {
		if last.Cfg.Locations[i].Path == "/" {
			rootLoc = &last.Cfg.Locations[i]
		}
	}
	if rootLoc == nil {
		t.Fatalf("merged config dropped '/' location: %+v", last.Cfg.Locations)
	}
	if rootLoc.ShieldID != "shield-abc" {
		t.Errorf("user-edited shield_id was not preserved on update: got %q", rootLoc.ShieldID)
	}
}

// containsString is a small slice-contains helper. We duplicate it across
// tests rather than depending on slices.Contains to keep imports stable.
func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
