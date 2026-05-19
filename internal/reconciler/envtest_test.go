package reconciler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// envtestSkipReason captures the reason we couldn't bring up the envtest
// control plane. When non-empty, every envtest case in this file is skipped
// via skipIfNoEnvtest. The most common reason in CI is that the
// `setup-envtest` binaries aren't installed locally.
var envtestSkipReason string

// testEnvCfg is the rest.Config produced by the envtest control plane. nil
// when envtest didn't start; never read by skipped tests.
var testEnvCfg *rest.Config

// TestMain bootstraps the envtest control plane once for the whole package.
// On failure we record the reason and let the rest of the suite run anyway —
// the Tier 1 unit tests in reconciler_test.go don't need envtest.
func TestMain(m *testing.M) {
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		envtestSkipReason = err.Error()
		os.Exit(m.Run())
	}
	testEnvCfg = cfg
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

// skipIfNoEnvtest is the gate every envtest case calls. Keeps the failure
// mode "skipped, not failed" when binaries aren't present locally.
func skipIfNoEnvtest(t *testing.T) {
	t.Helper()
	if envtestSkipReason != "" {
		t.Skipf("envtest not available: %s", envtestSkipReason)
	}
}

// startManager spins up a controller-runtime Manager wired with the supplied
// reconciler and starts it in a goroutine. Returns the cancel func so the
// test can stop the manager in a t.Cleanup.
func startManager(t *testing.T, r *IngressReconciler) (manager.Manager, context.CancelFunc) {
	t.Helper()
	mgr, err := ctrl.NewManager(testEnvCfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	r.Client = mgr.GetClient()
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := mgr.Start(ctx); err != nil {
			// Manager.Start returns nil on a clean shutdown via cancel; only
			// log if we actually exploded.
			t.Logf("manager exited: %v", err)
		}
	}()
	// Wait briefly for caches to sync. mgr.GetCache().WaitForCacheSync is the
	// canonical thing, but the manager only constructs the cache once Start
	// has run; a short ready-poll on a client.List is the simplest stand-in.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var probe networkingv1.IngressList
		if err := mgr.GetClient().List(ctx, &probe); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mgr, cancel
}

// envTestReconciler returns a fresh IngressReconciler with a fake API,
// configured to run against the envtest cluster.
func envTestReconciler() (*IngressReconciler, *fakeSiteAPI) {
	api := &fakeSiteAPI{}
	r := &IngressReconciler{
		APIClient:     api,
		IngressClass:  testIngressClass,
		ConnectorID:   testConnectorID,
		ConnectorSlug: testConnectorSlug,
		Log:           logr.Discard(),
	}
	return r, api
}

// uniqueName tags each envtest object with a per-test prefix so cases can run
// against the same shared control plane without colliding.
func uniqueName(t *testing.T, base string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano()%1000000)
}

// waitFor polls fn every 100ms up to timeout. Returns whether fn returned
// true within the window.
func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// TestEnvtest_FullReconcileFlow — create an Ingress + Service, expect the
// manager-driven reconcile to PutSite and set status hostname.
func TestEnvtest_FullReconcileFlow(t *testing.T) {
	skipIfNoEnvtest(t)
	r, api := envTestReconciler()
	_, cancel := startManager(t, r)
	defer cancel()

	ctx := context.Background()
	kc := r.Client
	svcName := uniqueName(t, "backend")
	ingName := uniqueName(t, "demo")
	host := uniqueName(t, "app") + ".example.com"

	if err := kc.Create(ctx, serviceFor("default", svcName, 80)); err != nil {
		t.Fatalf("create service: %v", err)
	}
	ing := ingressForTest("default", ingName, testIngressClass,
		ruleFor(host, "/", svcName, 80))
	if err := kc.Create(ctx, ing); err != nil {
		t.Fatalf("create ingress: %v", err)
	}

	if !waitFor(15*time.Second, func() bool {
		for _, c := range api.putSites() {
			if c.Host == host {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("PutSite never recorded for host %q (got %+v)", host, api.putSites())
	}

	// Status writeback must land too.
	if !waitFor(10*time.Second, func() bool {
		var got networkingv1.Ingress
		if err := kc.Get(ctx, types.NamespacedName{Namespace: "default", Name: ingName}, &got); err != nil {
			return false
		}
		for _, lb := range got.Status.LoadBalancer.Ingress {
			if lb.Hostname == host {
				return true
			}
		}
		return false
	}) {
		t.Errorf("status.loadBalancer.ingress never populated with host %q", host)
	}
}

// TestEnvtest_DeletionFlow — finalizer-driven cleanup: delete the Ingress;
// reconciler sees DeletionTimestamp; DeleteSite called; finalizer removed;
// object disappears.
func TestEnvtest_DeletionFlow(t *testing.T) {
	skipIfNoEnvtest(t)
	r, api := envTestReconciler()
	_, cancel := startManager(t, r)
	defer cancel()

	ctx := context.Background()
	kc := r.Client
	svcName := uniqueName(t, "backend")
	ingName := uniqueName(t, "demo")
	host := uniqueName(t, "delete") + ".example.com"

	if err := kc.Create(ctx, serviceFor("default", svcName, 80)); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := kc.Create(ctx, ingressForTest("default", ingName, testIngressClass,
		ruleFor(host, "/", svcName, 80))); err != nil {
		t.Fatalf("create ingress: %v", err)
	}

	// Wait for the first PutSite so we know the finalizer is in place.
	if !waitFor(15*time.Second, func() bool {
		for _, c := range api.putSites() {
			if c.Host == host {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("never saw initial PutSite")
	}

	// Delete the Ingress. The finalizer keeps the object alive until the
	// reconciler removes it.
	if err := kc.Delete(ctx, &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: ingName},
	}); err != nil {
		t.Fatalf("delete ingress: %v", err)
	}

	if !waitFor(15*time.Second, func() bool {
		for _, h := range api.deletes() {
			if h == host {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("DeleteSite never recorded for host %q", host)
	}

	// The object should disappear once the finalizer is gone.
	if !waitFor(10*time.Second, func() bool {
		var got networkingv1.Ingress
		err := kc.Get(ctx, types.NamespacedName{Namespace: "default", Name: ingName}, &got)
		return apierrors.IsNotFound(err)
	}) {
		t.Errorf("Ingress did not disappear after finalizer removal")
	}
}

// TestEnvtest_ServiceAppearsLater — Ingress created before its backend
// Service. The first reconcile is a no-op; once the Service appears the
// Service watcher retriggers reconciliation and PutSite lands.
func TestEnvtest_ServiceAppearsLater(t *testing.T) {
	skipIfNoEnvtest(t)
	r, api := envTestReconciler()
	_, cancel := startManager(t, r)
	defer cancel()

	ctx := context.Background()
	kc := r.Client
	svcName := uniqueName(t, "late")
	ingName := uniqueName(t, "demo")
	host := uniqueName(t, "late") + ".example.com"

	if err := kc.Create(ctx, ingressForTest("default", ingName, testIngressClass,
		ruleFor(host, "/", svcName, 80))); err != nil {
		t.Fatalf("create ingress: %v", err)
	}

	// Give the reconciler a chance to run against the missing service. We
	// can't easily observe "saw nothing" from outside, so just sleep a beat.
	time.Sleep(500 * time.Millisecond)

	// Now create the Service.
	if err := kc.Create(ctx, serviceFor("default", svcName, 80)); err != nil {
		t.Fatalf("create service: %v", err)
	}

	// The Service-watch fan-out should retrigger reconciliation; PutSite
	// should land within a reasonable window.
	if !waitFor(20*time.Second, func() bool {
		for _, c := range api.putSites() {
			if c.Host == host {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("PutSite never recorded after Service appeared (got %+v)", api.putSites())
	}
}

// _ = corev1.AddToScheme — ensures the corev1 import is retained even if no
// envtest case directly references it; the scheme already includes it but the
// blank import makes the dependency explicit.
var _ = corev1.AddToScheme
