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
// After the first EnsureConnector call, subsequent check-in responses include
// a `paired_connector` block so the reconciler can take its "credentials
// already exist" short-circuit path — matching how the real Bifrost API
// behaves and exercising the Step A skip logic.
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
			resp := map[string]any{
				"controller": map[string]any{
					"id":     "ctrl-uuid",
					"slug":   "demo",
					"name":   "Demo",
					"status": "active",
				},
			}
			if calls > 0 {
				resp["paired_connector"] = map[string]string{
					"id":   "cn-uuid",
					"slug": "test-connector",
				}
			}
			_ = json.NewEncoder(w).Encode(resp)
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

// stubEnroller returns a deterministic identity.json stub plus a counter the
// test can read to assert how many times enrollment ran. The bytes are unique
// enough that test assertions can compare exactly without touching Ziti.
func stubEnroller() (func(jwt string) ([]byte, error), *int) {
	calls := 0
	return func(jwt string) ([]byte, error) {
		calls++
		return []byte(`{"stub":"identity","jwt":"` + jwt + `"}`), nil
	}, &calls
}

// newBootstrapperForTest wires up a Bootstrapper backed by the fake K8s client
// and the supplied httptest API server. The Enroll function is stubbed so
// tests never reach out to a real Ziti controller.
func newBootstrapperForTest(t *testing.T, apiSrv *httptest.Server, ns string, initObjs ...client.Object) (*Bootstrapper, client.Client, *int) {
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
	enroll, enrollCalls := stubEnroller()
	return &Bootstrapper{API: api, Cfg: cfg, Client: kc, Enroll: enroll}, kc, enrollCalls
}

// TestReconcile_CreatesSecretAndDeployment — the happy path from an empty
// cluster: Secret and Deployment are both created with the expected fields.
// In particular the Secret carries the identity.json the stub enroller
// returned, and the Deployment mounts it as a file at the agreed path.
func TestReconcile_CreatesSecretAndDeployment(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc, enrollCalls := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if *enrollCalls != 1 {
		t.Errorf("expected exactly one Enroll call on first bootstrap, got %d", *enrollCalls)
	}

	var secret corev1.Secret
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&secret); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	for _, key := range []string{"INGRESSIVE_API_URL", "INGRESSIVE_API_KEY_ID", "INGRESSIVE_API_KEY_SECRET"} {
		if _, ok := secret.StringData[key]; !ok {
			t.Errorf("secret missing scalar key %s", key)
		}
	}
	if _, ok := secret.StringData["ENROLLMENT_JWT"]; ok {
		t.Error("ENROLLMENT_JWT must no longer be written to the Secret")
	}
	wantIdentity := []byte(`{"stub":"identity","jwt":"JWT.value"}`)
	if got := secret.Data[identitySecretKey]; string(got) != string(wantIdentity) {
		t.Errorf("secret.Data[%q] = %q, want %q", identitySecretKey, got, wantIdentity)
	}

	var dep appsv1.Deployment
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	c := dep.Spec.Template.Spec.Containers[0]
	if c.Image != "ghcr.io/test/connector:v1" {
		t.Errorf("deployment image = %q", c.Image)
	}
	if len(c.EnvFrom) != 1 {
		t.Errorf("expected envFrom with the Secret reference")
	} else if name := c.EnvFrom[0].SecretRef.Name; name != secret.Name {
		t.Errorf("envFrom.secretRef.name = %q, want %q", name, secret.Name)
	}
	if !hasIdentityDirEnv(c.Env) {
		t.Errorf("deployment container missing INGRESSIVE_IDENTITY_DIR=%s env", identityMountDir)
	}
	if !hasIdentityVolumeMount(c.VolumeMounts) {
		t.Errorf("deployment container missing identity volumeMount at %s", identityMountDir)
	}
	if !hasIdentityVolume(dep.Spec.Template.Spec.Volumes) {
		t.Error("deployment pod spec missing identity Secret volume")
	}
}

// TestReconcile_NoopWhenStateMatches — a second Reconcile call after the
// first must not produce changes detectable in the object's ResourceVersion.
// The fake client increments ResourceVersion on Update; comparing before/after
// is the simplest reliable signal. Also asserts the enroller stub is not
// called a second time — drift reconciles must never touch Ziti.
func TestReconcile_NoopWhenStateMatches(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc, enrollCalls := newBootstrapperForTest(t, srv, "ingressive-system")

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
	if *enrollCalls != 1 {
		t.Errorf("expected enroller to run exactly once across two reconciles, got %d", *enrollCalls)
	}
}

// TestReconcile_SecretRecreatedAfterDeletion — drift recovery: if the Secret
// is deleted out-of-band, the next reconcile must recreate it (re-minting
// credentials and re-enrolling the identity). This is what the periodic loop
// exists for.
func TestReconcile_SecretRecreatedAfterDeletion(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc, enrollCalls := newBootstrapperForTest(t, srv, "ingressive-system")

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
	if len(sec.Data[identitySecretKey]) == 0 {
		t.Error("recreated Secret missing identity.json")
	}
	if *enrollCalls != 2 {
		t.Errorf("expected enroller to run once per Secret mint (2 total), got %d", *enrollCalls)
	}
}

// TestReconcile_DeploymentUpdatedWhenImageChanges — operator bumps the
// configured connector image; the existing Deployment must be updated.
func TestReconcile_DeploymentUpdatedWhenImageChanges(t *testing.T) {
	srv, _ := fakeAPIServer(t, nil)
	defer srv.Close()
	bs, kc, _ := newBootstrapperForTest(t, srv, "ingressive-system")

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

// TestReconcile_SecretRotatedOnDeleteAndReMint — drift reconciles never
// rotate credentials on their own, but an out-of-band Secret deletion forces
// a fresh EnsureConnector + Enroll. The next reconcile must publish the new
// access key and the freshly-enrolled identity.json.
func TestReconcile_SecretRotatedOnDeleteAndReMint(t *testing.T) {
	srv, ensureCalls := fakeAPIServer(t, func(call int, creds map[string]any) {
		if call >= 2 {
			creds["access_key"] = map[string]string{"id": "BFAKKEY2", "secret": "secret2"}
			creds["enrollment_jwt"] = "JWT2"
		}
	})
	defer srv.Close()
	bs, kc, enrollCalls := newBootstrapperForTest(t, srv, "ingressive-system")

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Force the re-mint path by deleting the Secret out-of-band.
	if err := kc.Delete(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
	}); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if *ensureCalls != 2 {
		t.Errorf("expected exactly 2 EnsureConnector calls (initial + re-mint), got %d", *ensureCalls)
	}
	if *enrollCalls != 2 {
		t.Errorf("expected exactly 2 Enroll calls (initial + re-mint), got %d", *enrollCalls)
	}

	var sec corev1.Secret
	_ = kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"}, &sec)
	if got := sec.StringData["INGRESSIVE_API_KEY_ID"]; got != "BFAKKEY2" {
		t.Errorf("expected key rotated to BFAKKEY2, got %q", got)
	}
	wantIdentity := []byte(`{"stub":"identity","jwt":"JWT2"}`)
	if got := sec.Data[identitySecretKey]; string(got) != string(wantIdentity) {
		t.Errorf("identity.json after re-mint = %q, want %q", got, wantIdentity)
	}
}

// TestReconcile_MigratesPreIdentitySecret — a Secret left behind by a
// pre-migration controller has the scalar env vars but no identity.json key,
// and the server's check-in advertises a paired_connector (so Step A would
// normally skip EnsureConnector). The reconciler must detect the missing
// identity.json, re-mint via EnsureConnector + Enroll, and replace the legacy
// Secret with one that includes identity.json.
func TestReconcile_MigratesPreIdentitySecret(t *testing.T) {
	legacy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ingressive-system",
			Name:      "ingressive-connector-test-connector",
			Labels:    labelsFor("test-connector"),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"INGRESSIVE_API_URL":        []byte("https://old.example"),
			"INGRESSIVE_API_KEY_ID":     []byte("OLDKEY"),
			"INGRESSIVE_API_KEY_SECRET": []byte("oldsecret"),
			"ENROLLMENT_JWT":            []byte("OLDJWT"),
		},
	}

	// Hand-rolled fake API: check-in always returns paired_connector to
	// exercise the Step-A-with-stale-Secret branch specifically.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/controller/check-in" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"controller": map[string]any{
					"id":     "ctrl-uuid",
					"slug":   "demo",
					"name":   "Demo",
					"status": "active",
				},
				"paired_connector": map[string]string{
					"id":   "cn-uuid",
					"slug": "test-connector",
				},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
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
		})
	}))
	defer srv.Close()

	bs, kc, enrollCalls := newBootstrapperForTest(t, srv, "ingressive-system", legacy)

	if _, err := bs.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if *enrollCalls != 1 {
		t.Errorf("expected one Enroll call for migration, got %d", *enrollCalls)
	}

	var sec corev1.Secret
	if err := kc.Get(context.Background(),
		types.NamespacedName{Namespace: "ingressive-system", Name: "ingressive-connector-test-connector"},
		&sec); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if len(sec.Data[identitySecretKey]) == 0 {
		t.Error("migrated Secret missing identity.json")
	}
}
