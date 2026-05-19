package translate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiclient "github.com/ingressive-cloud/controller/internal/api"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/yaml"
)

// fixtureConnectorID is the value we stamp into every Upstream during tests.
// expected.json files reference this string literally; keep them in sync if
// you change it.
const fixtureConnectorID = "cn-fixture-uuid"

// TestTranslate_TableDriven discovers every subdirectory under testdata/ and
// runs the same flow against it. Each subdir contains:
//
//   ingress.yaml   — the input Ingress
//   services.yaml  — fixture Services the service-fetch closure resolves against
//   expected.json  — the expected map[host]SiteConfiguration
//
// Adding a new case is just dropping a new directory with those three files.
func TestTranslate_TableDriven(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join("testdata", name)
			ing := loadIngress(t, filepath.Join(dir, "ingress.yaml"))
			services := loadServices(t, filepath.Join(dir, "services.yaml"))

			var expected map[string]apiclient.SiteConfiguration
			expBytes, err := os.ReadFile(filepath.Join(dir, "expected.json"))
			if err != nil {
				t.Fatalf("read expected.json: %v", err)
			}
			if err := json.Unmarshal(expBytes, &expected); err != nil {
				t.Fatalf("decode expected.json: %v", err)
			}

			got, err := Translate(context.Background(), ing, fixtureConnectorID,
				fixtureServiceFetch(services), discardLogger())
			if err != nil {
				t.Fatalf("Translate: %v", err)
			}
			if diff := cmp.Diff(expected, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTranslate_ServiceNotFound — a backend references a Service that the
// fetch closure can't resolve. Translate must return a typed
// ServiceNotFoundError so the reconciler can react.
func TestTranslate_ServiceNotFound(t *testing.T) {
	ing := ingressWithNamedPort("missing-service", "http")
	_, err := Translate(context.Background(), ing, fixtureConnectorID,
		fixtureServiceFetch(nil), discardLogger())
	var notFound *ServiceNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *ServiceNotFoundError, got %T: %v", err, err)
	}
	if notFound.Name != "missing-service" || notFound.Namespace != "default" {
		t.Errorf("error fields wrong: %+v", notFound)
	}
}

// TestTranslate_PortNotFound — the Service exists but does not expose the
// named port the Ingress asks for.
func TestTranslate_PortNotFound(t *testing.T) {
	svc := &corev1.Service{}
	svc.Namespace = "default"
	svc.Name = "api"
	svc.Spec.Ports = []corev1.ServicePort{{Name: "http", Port: 8080}}
	ing := ingressWithNamedPort("api", "grpc")

	_, err := Translate(context.Background(), ing, fixtureConnectorID,
		fixtureServiceFetch([]*corev1.Service{svc}), discardLogger())
	var portErr *PortNotFoundError
	if !errors.As(err, &portErr) {
		t.Fatalf("expected *PortNotFoundError, got %T: %v", err, err)
	}
	if portErr.PortName != "grpc" || portErr.ServiceName != "api" {
		t.Errorf("error fields wrong: %+v", portErr)
	}
}

// TestTranslate_UnknownAnnotationLogsWarning — an irrelevant
// nginx.ingress.kubernetes.io/* annotation must be logged at WARN but not fail
// the translation.
func TestTranslate_UnknownAnnotationLogsWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ing := &networkingv1.Ingress{}
	ing.Namespace = "default"
	ing.Name = "demo"
	ing.Annotations = map[string]string{
		"nginx.ingress.kubernetes.io/affinity": "cookie",
	}
	pt := networkingv1.PathTypePrefix
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{
				Path: "/", PathType: &pt,
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
					Name: "api", Port: networkingv1.ServiceBackendPort{Number: 80},
				}},
			}},
		}},
	}}

	if _, err := Translate(context.Background(), ing, fixtureConnectorID, stubFetcher, logger); err != nil {
		t.Fatalf("Translate: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "unknown nginx.ingress.kubernetes.io annotation") {
		t.Errorf("expected unknown-annotation warning, got %q", out)
	}
	if !strings.Contains(out, "affinity") {
		t.Errorf("warning should include annotation key: %q", out)
	}
}

// TestTranslate_TLSWarning — spec.tls is recognized but not translated; logs
// a warning so operators know it's ignored.
func TestTranslate_TLSWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ing := &networkingv1.Ingress{}
	ing.Namespace = "default"
	ing.Name = "demo"
	ing.Spec.TLS = []networkingv1.IngressTLS{{Hosts: []string{"a.example.com"}, SecretName: "tls"}}
	pt := networkingv1.PathTypePrefix
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "a.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{
				Path: "/", PathType: &pt,
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
					Name: "api", Port: networkingv1.ServiceBackendPort{Number: 80},
				}},
			}},
		}},
	}}
	if _, err := Translate(context.Background(), ing, fixtureConnectorID, stubFetcher, logger); err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(buf.String(), "spec.tls is ignored") {
		t.Errorf("expected TLS warning, got %q", buf.String())
	}
}

// TestTranslate_ConfigurationSnippetWarning — we refuse arbitrary nginx config
// but say so explicitly rather than lumping it in with "unknown annotation".
func TestTranslate_ConfigurationSnippetWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ing := &networkingv1.Ingress{}
	ing.Namespace = "default"
	ing.Name = "demo"
	ing.Annotations = map[string]string{
		"nginx.ingress.kubernetes.io/configuration-snippet": "more_set_headers \"X-Foo: bar\";",
	}
	pt := networkingv1.PathTypePrefix
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "a.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{
				Path: "/", PathType: &pt,
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
					Name: "api", Port: networkingv1.ServiceBackendPort{Number: 80},
				}},
			}},
		}},
	}}
	if _, err := Translate(context.Background(), ing, fixtureConnectorID, stubFetcher, logger); err != nil {
		t.Fatalf("Translate: %v", err)
	}
	if !strings.Contains(buf.String(), "configuration-snippet") ||
		!strings.Contains(buf.String(), "not supported") {
		t.Errorf("expected configuration-snippet refusal warning, got %q", buf.String())
	}
}

// fixtureServiceFetch returns a ServiceFetcher backed by a static map. Tests
// load Services from a YAML fixture and pass them through here.
func fixtureServiceFetch(svcs []*corev1.Service) ServiceFetcher {
	idx := make(map[string]*corev1.Service)
	for _, s := range svcs {
		idx[s.Namespace+"/"+s.Name] = s
	}
	return func(_ context.Context, namespace, name string) (*corev1.Service, error) {
		s, ok := idx[namespace+"/"+name]
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		return s, nil
	}
}

// nilFetcher always reports not-found. Used by tests that assert Service-
// not-found behavior; routes that need a Service to translate should use
// fixtureServiceFetch or stubFetcher.
func nilFetcher(_ context.Context, _, _ string) (*corev1.Service, error) {
	return nil, fmt.Errorf("not found")
}

// stubFetcher returns a placeholder Service for any lookup. Tests that just
// need the Service existence check to pass (so they can assert on something
// else, like annotation warnings) use this rather than crafting fixtures.
func stubFetcher(_ context.Context, ns, name string) (*corev1.Service, error) {
	svc := &corev1.Service{}
	svc.Namespace = ns
	svc.Name = name
	svc.Spec.Ports = []corev1.ServicePort{{Port: 80}}
	return svc, nil
}

// discardLogger silences slog output for tests that aren't asserting on it.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// loadIngress decodes a single Ingress from a YAML file.
func loadIngress(t *testing.T, path string) *networkingv1.Ingress {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	obj := &networkingv1.Ingress{}
	if err := yaml.Unmarshal(raw, obj); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return obj
}

// loadServices decodes a YAML stream of Service objects (`---`-separated).
// Returns nil if the file doesn't exist (some cases use numeric ports only
// and don't need any fixture Services).
func loadServices(t *testing.T, path string) []*corev1.Service {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read %s: %v", path, err)
	}
	var out []*corev1.Service
	for _, doc := range bytes.Split(raw, []byte("\n---")) {
		doc = bytes.TrimSpace(doc)
		if len(doc) == 0 {
			continue
		}
		svc := &corev1.Service{}
		if err := yaml.Unmarshal(doc, svc); err != nil {
			t.Fatalf("decode service from %s: %v", path, err)
		}
		out = append(out, svc)
	}
	return out
}

// ingressWithNamedPort produces an Ingress in the default namespace that
// references a Service by name with a port referenced by name.
func ingressWithNamedPort(svcName, portName string) *networkingv1.Ingress {
	ing := &networkingv1.Ingress{}
	ing.Namespace = "default"
	ing.Name = "demo"
	pt := networkingv1.PathTypePrefix
	ing.Spec.Rules = []networkingv1.IngressRule{{
		Host: "app.example.com",
		IngressRuleValue: networkingv1.IngressRuleValue{HTTP: &networkingv1.HTTPIngressRuleValue{
			Paths: []networkingv1.HTTPIngressPath{{
				Path: "/", PathType: &pt,
				Backend: networkingv1.IngressBackend{Service: &networkingv1.IngressServiceBackend{
					Name: svcName,
					Port: networkingv1.ServiceBackendPort{Name: portName},
				}},
			}},
		}},
	}}
	return ing
}

