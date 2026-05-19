package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
	"github.com/ingressive-cloud/controller/internal/config"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testScheme returns a scheme with Core + Apps v1 registered, which is what
// the fake controller-runtime client needs to handle Secrets and Deployments.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("add to scheme: %v", err)
	}
	return s
}

// fakeAPIServer returns an httptest server that handles both endpoints the
// bootstrap reconciler calls in order:
//
//   - POST /controller/check-in       → controller identity (returns slug "demo")
//   - POST /controllers/demo/connector → connector credentials
//
// `mutate` is invoked on every connector-credentials response so tests can
// rotate the returned values between reconciles. `calls` counts only the
// EnsureConnector invocations (the credential-bearing responses) for tests
// that assert on rotation behavior.
func fakeAPIServer(t *testing.T, mutate func(call int, creds map[string]any)) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/controller/check-in":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"controller": map[string]any{
					"id":     "ctrl-uuid",
					"slug":   "demo",
					"name":   "Demo",
					"status": "active",
				},
			})
		default:
			// Assume any other path is the connector-bootstrap endpoint.
			calls++
			resp := map[string]any{
				"connector": map[string]string{
					"id":   "cn-uuid",
					"slug": "test-connector",
					"name": "Test",
				},
				"access_key": map[string]string{
					"id":     "BFAKKEY",
					"secret": "secret",
				},
				"enrollment_jwt": "JWT.value",
			}
			if mutate != nil {
				mutate(calls, resp)
			}
			_ = json.NewEncoder(w).Encode(resp)
		}
	}))
	return srv, &calls
}

// newBootstrapperForTest wires up a Bootstrapper backed by the fake K8s client
// and the supplied httptest API server.
func newBootstrapperForTest(t *testing.T, apiSrv *httptest.Server, ns string, initObjs ...client.Object) (*Bootstrapper, client.Client) {
	t.Helper()
	cfg := &config.Config{
		APIURL:         apiSrv.URL,
		APIKeyID:       "BFAKKEY",
		APIKeySecret:   "secret",
		ConnectorImage: "ghcr.io/test/connector:v1",
		Namespace:      ns,
	}
	api := apiclient.New(cfg.APIURL, cfg.APIKeyID, cfg.APIKeySecret, "0.0.0-test")
	kc := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(initObjs...).Build()
	return &Bootstrapper{API: api, Cfg: cfg, Client: kc}, kc
}

// TestReconcile_CreatesSecretAndDeployment — the happy path from an empty
// cluster: Secret and Deployment are both created with the expected fields.
func TestReconcile_CreatesSecretAndDeployment(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var secret corev1.Secret
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&secret); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	for _, key := range []string{"INGRESSIVE_API_URL", "INGRESSIVE_API_KEY_ID", "INGRESSIVE_API_KEY_SECRET", "ENROLLMENT_JWT"} {
		if _, ok := secret.StringData[key]; !ok {
			t.Errorf("secret missing key %s", key)
		}
	}

	var dep appsv1.Deployment
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Template.Spec.Containers[0].Image != "ghcr.io/test/connector:v1" {
		t.Errorf("deployment image = %q", dep.Spec.Template.Spec.Containers[0].Image)
	}
	if len(dep.Spec.Template.Spec.Containers[0].EnvFrom) != 1 {
		t.Errorf("expected envFrom with the Secret reference")
	} else if name := dep.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name; name != secret.Name {
		t.Errorf("envFrom.secretRef.name = %q, want %q", name, secret.Name)
	}
}

// TestReconcile_NoopWhenStateMatches — a second Reconcile call after the
// first must not produce changes detectable in the object's ResourceVersion.
// The fake client increments ResourceVersion on Update; comparing before/after
// is the simplest reliable signal.
func TestReconcile_NoopWhenStateMatches(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	var sec1 corev1.Secret
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &sec1)
	var dep1 appsv1.Deployment
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &dep1)

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	var dep2 appsv1.Deployment
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &dep2)
	if dep1.ResourceVersion != dep2.ResourceVersion {
		t.Errorf("Deployment was rewritten on a no-op reconcile (rv %q -> %q)", dep1.ResourceVersion, dep2.ResourceVersion)
	}
}

// TestReconcile_SecretRecreatedAfterDeletion — drift recovery: if the Secret
// is deleted out-of-band, the next reconcile must recreate it. This is what
// the periodic loop exists for.
func TestReconcile_SecretRecreatedAfterDeletion(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Delete the Secret out-of-band.
	if err := kc.Delete(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
	}); err != nil {
		t.Fatalf("delete secret: %v", err)
	}

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	var sec corev1.Secret
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&sec); err != nil {
		if kerrors.IsNotFound(err) {
			t.Fatal("secret was not recreated after deletion")
		}
		t.Fatalf("get secret: %v", err)
	}
}

// TestReconcile_DeploymentUpdatedWhenImageChanges — operator bumps the
// configured connector image; the existing Deployment must be updated.
func TestReconcile_DeploymentUpdatedWhenImageChanges(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	bs.Cfg.ConnectorImage = "ghcr.io/test/connector:v2"
	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile after image bump: %v", err)
	}
	var dep appsv1.Deployment
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &dep)
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "ghcr.io/test/connector:v2" {
		t.Errorf("image after bump = %q, want v2", got)
	}
}

// TestReconcile_SecretUpdatedWhenCredentialsChange — the API rotates
// credentials on every EnsureConnector call. The Secret must move forward in
// step with what the server returned.
func TestReconcile_SecretUpdatedWhenCredentialsChange(t *testing.T) {
	srv, calls := fakeAPIServer(t, func(call int, creds map[string]any) {
		if call >= 2 {
			creds["access_key"] = map[string]string{"id": "BFAKKEY2", "secret": "secret2"}
			creds["enrollment_jwt"] = "JWT2"
		}
	})
	defer srv.Close()
	bs, kc := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if *calls < 2 {
		t.Fatalf("expected at least 2 EnsureConnector calls, got %d", *calls)
	}

	var sec corev1.Secret
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &sec)
	// The fake client surfaces fresh StringData on the next Get following an
	// Update, so we read from StringData here.
	if got := sec.StringData["INGRESSIVE_API_KEY_ID"]; got != "BFAKKEY2" {
		t.Errorf("expected key rotated to BFAKKEY2, got %q", got)
	}
	if got := sec.StringData["ENROLLMENT_JWT"]; got != "JWT2" {
		t.Errorf("expected JWT rotated, got %q", got)
	}
}
