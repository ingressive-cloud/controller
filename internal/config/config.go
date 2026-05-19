// Package config loads the controller's runtime configuration from CLI flags
// and environment variables. Env vars are used as flag defaults so an operator
// can configure the controller either way (Helm renders env vars; ad-hoc runs
// use flags).
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

// defaults that we expose so docs/tests can reference them.
const (
	DefaultAPIURL         = "https://console.ingressive.cloud"
	DefaultConnectorImage = "ghcr.io/ingressive-cloud/connector:latest"
	DefaultNamespace      = "default"
	DefaultIngressClass   = "ingressive"

	// serviceAccountNamespacePath is the in-pod file injected by Kubernetes
	// when a ServiceAccount token is mounted. Used to autodetect the install
	// namespace.
	serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// Config is the resolved runtime configuration. Required fields are documented
// inline; missing required fields cause Load to fail.
//
// The controller's own slug is intentionally NOT stored here — it is
// discovered at runtime via POST /controller/check-in, which resolves it from
// the API credential. The operator only configures URL + credentials.
type Config struct {
	// APIURL is the Bifrost API root (without a trailing slash).
	APIURL string

	// APIKeyID and APIKeySecret authenticate the controller as its scoped
	// service user. Created once on the server and rendered into a K8s Secret
	// by the controller's Helm chart.
	APIKeyID     string
	APIKeySecret string

	// ConnectorImage is the container image used for the paired-connector
	// Deployment the controller creates.
	ConnectorImage string

	// Namespace is the namespace the controller deploys its paired connector
	// into. The connector always lives in the same namespace as the controller
	// (no cross-namespace deployments in v1).
	Namespace string

	// IngressClass is the IngressClass name (spec.ingressClassName) the
	// controller reconciles. Defaults to "ingressive". The matching
	// IngressClass resource is created by the Helm chart with
	// spec.controller = ingressive.cloud/controller.
	IngressClass string
}

// Load parses os.Args[1:] and the environment. It returns a fully-resolved
// Config or an error if a required value is missing.
func Load() (*Config, error) {
	return loadFrom(os.Args[1:], os.Getenv)
}

// loadFrom is the testable form of Load. Pass args and a getenv function;
// arguments take precedence over env vars (because we read env into the flag
// defaults, then let flag.Parse overwrite).
func loadFrom(args []string, getenv func(string) string) (*Config, error) {
	fs := flag.NewFlagSet("ingressive-controller", flag.ContinueOnError)
	// Silence the default Usage spam on -h; we'll handle it explicitly.
	fs.SetOutput(os.Stderr)

	apiURL := fs.String("api-url", envOr(getenv, "INGRESSIVE_API_URL", DefaultAPIURL),
		"Ingressive API base URL")
	keyID := fs.String("api-key-id", getenv("INGRESSIVE_API_KEY_ID"),
		"BFAK access key ID (required; env INGRESSIVE_API_KEY_ID)")
	keySecret := fs.String("api-key-secret", getenv("INGRESSIVE_API_KEY_SECRET"),
		"Access key secret (required; env INGRESSIVE_API_KEY_SECRET)")
	connectorImage := fs.String("connector-image", envOr(getenv, "CONNECTOR_IMAGE", DefaultConnectorImage),
		"Container image for the paired connector Deployment")
	namespace := fs.String("namespace", envOr(getenv, "POD_NAMESPACE", autodetectNamespace()),
		"Kubernetes namespace to deploy the paired connector into")
	ingressClass := fs.String("ingress-class", envOr(getenv, "INGRESS_CLASS", DefaultIngressClass),
		"IngressClass name (spec.ingressClassName) this controller reconciles")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg := &Config{
		APIURL:         strings.TrimRight(strings.TrimSpace(*apiURL), "/"),
		APIKeyID:       strings.TrimSpace(*keyID),
		APIKeySecret:   strings.TrimSpace(*keySecret),
		ConnectorImage: strings.TrimSpace(*connectorImage),
		Namespace:      strings.TrimSpace(*namespace),
		IngressClass:   strings.TrimSpace(*ingressClass),
	}

	var missing []string
	if cfg.APIKeyID == "" {
		missing = append(missing, "--api-key-id / INGRESSIVE_API_KEY_ID")
	}
	if cfg.APIKeySecret == "" {
		missing = append(missing, "--api-key-secret / INGRESSIVE_API_KEY_SECRET")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}
	if cfg.APIURL == "" {
		return nil, errors.New("api-url must not be empty")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
	if cfg.IngressClass == "" {
		cfg.IngressClass = DefaultIngressClass
	}
	return cfg, nil
}

// envOr returns getenv(key) if non-empty, else def. Lets flag defaults seed
// themselves from the environment.
func envOr(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}

// autodetectNamespace returns the contents of the in-pod namespace file when
// available, or DefaultNamespace as a fallback. Used as the flag default for
// --namespace.
func autodetectNamespace() string {
	b, err := os.ReadFile(serviceAccountNamespacePath)
	if err != nil {
		return DefaultNamespace
	}
	ns := strings.TrimSpace(string(b))
	if ns == "" {
		return DefaultNamespace
	}
	return ns
}
