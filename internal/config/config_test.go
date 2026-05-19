package config

import (
	"strings"
	"testing"
)

// stubEnv lets tests pin the environment without touching the real one.
func stubEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadFrom_AllFromEnv(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_URL":        "https://api.example",
		"INGRESSIVE_API_KEY_ID":     "BFAK123",
		"INGRESSIVE_API_KEY_SECRET": "secret",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.APIURL != "https://api.example" {
		t.Errorf("APIURL = %q", cfg.APIURL)
	}
	if cfg.APIKeyID != "BFAK123" || cfg.APIKeySecret != "secret" {
		t.Errorf("creds not loaded: %+v", cfg)
	}
	// Connector image must fall back to the bundled default.
	if cfg.ConnectorImage != DefaultConnectorImage {
		t.Errorf("ConnectorImage default = %q", cfg.ConnectorImage)
	}
}

func TestLoadFrom_FlagsOverrideEnv(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "BFAKenv",
		"INGRESSIVE_API_KEY_SECRET": "secretenv",
	})
	args := []string{
		"-api-url", "https://override",
		"-connector-image", "registry.example/connector:v9",
	}
	cfg, err := loadFrom(args, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.APIURL != "https://override" {
		t.Errorf("APIURL = %q", cfg.APIURL)
	}
	if cfg.ConnectorImage != "registry.example/connector:v9" {
		t.Errorf("image = %q", cfg.ConnectorImage)
	}
}

func TestLoadFrom_MissingRequiredFails(t *testing.T) {
	env := stubEnv(map[string]string{})
	_, err := loadFrom(nil, env)
	if err == nil {
		t.Fatal("expected error for missing required values")
	}
	// Error must enumerate what's missing so operators can fix it.
	for _, want := range []string{"api-key-id", "api-key-secret"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing reference to %s", err.Error(), want)
		}
	}
}

func TestLoadFrom_TrimsTrailingSlashFromAPIURL(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_URL":        "https://api.example///",
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.APIURL != "https://api.example" {
		t.Errorf("expected trailing slashes stripped, got %q", cfg.APIURL)
	}
}

func TestLoadFrom_DefaultsAPIURL(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.APIURL != DefaultAPIURL {
		t.Errorf("APIURL default = %q", cfg.APIURL)
	}
}

func TestLoadFrom_NamespaceFromEnv(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
		"POD_NAMESPACE":             "ingressive-system",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.Namespace != "ingressive-system" {
		t.Errorf("Namespace = %q, want ingressive-system", cfg.Namespace)
	}
}

// TestLoadFrom_IngressClassDefault — when the operator supplies nothing, the
// controller still has a sensible class name to watch for.
func TestLoadFrom_IngressClassDefault(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.IngressClass != DefaultIngressClass {
		t.Errorf("IngressClass default = %q, want %q", cfg.IngressClass, DefaultIngressClass)
	}
}

// TestLoadFrom_IngressClassFromEnv — env var feeds the flag default.
func TestLoadFrom_IngressClassFromEnv(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
		"INGRESS_CLASS":             "custom-class",
	})
	cfg, err := loadFrom(nil, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.IngressClass != "custom-class" {
		t.Errorf("IngressClass = %q, want custom-class", cfg.IngressClass)
	}
}

// TestLoadFrom_IngressClassFlagOverride — explicit flag wins over env.
func TestLoadFrom_IngressClassFlagOverride(t *testing.T) {
	env := stubEnv(map[string]string{
		"INGRESSIVE_API_KEY_ID":     "k",
		"INGRESSIVE_API_KEY_SECRET": "s",
		"INGRESS_CLASS":             "from-env",
	})
	cfg, err := loadFrom([]string{"-ingress-class", "from-flag"}, env)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}
	if cfg.IngressClass != "from-flag" {
		t.Errorf("IngressClass = %q, want from-flag", cfg.IngressClass)
	}
}
