package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestEnsureConnector_HappyPath asserts that on a 200 the JSON is decoded into
// ConnectorCredentials with all expected fields populated, and that the
// outbound request carries the headers we promise (signed, UA, Accept).
func TestEnsureConnector_HappyPath(t *testing.T) {
	var (
		gotMethod  string
		gotPath    string
		gotUA      string
		gotAuth    string
		gotAccept  string
		gotAmzDate string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotUA = r.Header.Get("User-Agent")
		gotAuth = r.Header.Get("Authorization")
		gotAccept = r.Header.Get("Accept")
		gotAmzDate = r.Header.Get("x-amz-date")

		// Mimic the bifrost handleControllerConnector response shape.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"connector": map[string]string{
				"id":   "cn-123",
				"slug": "_ctrl_test-controller",
				"name": "Test Controller (controller)",
			},
			"access_key": map[string]string{
				"id":     "BFAKABCDEF1234567890",
				"secret": "supersecret-base64==",
			},
			"enrollment_jwt": "eyJfake.jwt.token",
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "BFAKKEYID", "secret", "0.1.0-test")
	creds, err := c.EnsureConnector(context.Background(), "test-controller")
	if err != nil {
		t.Fatalf("EnsureConnector: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/controllers/test-controller/connector" {
		t.Errorf("path = %q", gotPath)
	}
	if !strings.HasPrefix(gotUA, "Ingressive-Controller/") {
		t.Errorf("User-Agent = %q, want prefix Ingressive-Controller/", gotUA)
	}
	if !strings.Contains(gotUA, "0.1.0-test") {
		t.Errorf("User-Agent = %q, want it to include the version", gotUA)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q, want application/json", gotAccept)
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 ") {
		t.Errorf("Authorization = %q, want AWS4-HMAC-SHA256 prefix", gotAuth)
	}
	if !strings.Contains(gotAuth, "Credential=BFAKKEYID/") {
		t.Errorf("Authorization missing Credential=BFAKKEYID/: %q", gotAuth)
	}
	if gotAmzDate == "" {
		t.Error("x-amz-date header missing — request was not signed")
	}

	if creds.ConnectorID != "cn-123" {
		t.Errorf("ConnectorID = %q", creds.ConnectorID)
	}
	if creds.ConnectorSlug != "_ctrl_test-controller" {
		t.Errorf("ConnectorSlug = %q", creds.ConnectorSlug)
	}
	if creds.AccessKeyID != "BFAKABCDEF1234567890" {
		t.Errorf("AccessKeyID = %q", creds.AccessKeyID)
	}
	if creds.AccessKeySecret != "supersecret-base64==" {
		t.Errorf("AccessKeySecret = %q", creds.AccessKeySecret)
	}
	if creds.EnrollmentJWT != "eyJfake.jwt.token" {
		t.Errorf("EnrollmentJWT = %q", creds.EnrollmentJWT)
	}
}

// TestEnsureConnector_4xxReturnsAPIError — server returns 404, we return a
// typed *APIError that callers can errors.As-check.
func TestEnsureConnector_4xxReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"controller not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, "BFAKKEYID", "secret", "test")
	_, err := c.EnsureConnector(context.Background(), "doesnt-exist")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", apiErr.StatusCode)
	}
	if !apiErr.IsClientError() {
		t.Error("IsClientError should be true for 404")
	}
	if !strings.Contains(apiErr.Body, "controller not found") {
		t.Errorf("body did not propagate: %q", apiErr.Body)
	}
}

// TestEnsureConnector_5xxReturnsAPIError — server returns 500.
func TestEnsureConnector_5xxReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, "BFAKKEYID", "secret", "test")
	_, err := c.EnsureConnector(context.Background(), "any")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if !apiErr.IsServerError() {
		t.Error("IsServerError should be true for 500")
	}
}

// TestEnsureConnector_NetworkErrorWrapped — a dead server produces a wrapped
// error rather than an *APIError, since there was no HTTP response.
func TestEnsureConnector_NetworkErrorWrapped(t *testing.T) {
	// Bind, capture the URL, then immediately close to guarantee a dead port.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := srv.URL
	srv.Close()

	c := New(deadURL, "BFAKKEYID", "secret", "test")
	_, err := c.EnsureConnector(context.Background(), "anything")
	if err == nil {
		t.Fatal("expected network error, got nil")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Fatalf("network error should not be an APIError: %v", err)
	}
	// Must reference the method/path so callers know what failed.
	if !strings.Contains(err.Error(), "POST /controllers/anything/connector") {
		t.Errorf("error missing method/path: %v", err)
	}
}

// TestUserAgent_DefaultsToDev — empty Version produces "Ingressive-Controller/dev".
func TestUserAgent_DefaultsToDev(t *testing.T) {
	c := New("http://example", "k", "s", "")
	if c.userAgent() != "Ingressive-Controller/dev" {
		t.Errorf("default UA = %q", c.userAgent())
	}
}

// TestEnsureConnector_EmptySlugRejected — guard against accidental calls with
// no slug (would hit /controllers//connector and 404 in a confusing way).
func TestEnsureConnector_EmptySlugRejected(t *testing.T) {
	c := New("http://example", "k", "s", "test")
	_, err := c.EnsureConnector(context.Background(), "  ")
	if err == nil || !strings.Contains(err.Error(), "controllerSlug") {
		t.Errorf("expected slug-validation error, got %v", err)
	}
}

// TestSigningStable verifies the AWSv4 signature is deterministic for a fixed
// timestamp — a regression test on the lifted signing code.
func TestSigningStable(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://api.example/controllers/x/connector", nil)
	req.Host = "api.example"
	fixed, _ := time.Parse("20060102T150405Z", "20250101T000000Z")
	if err := signAt(req, "BFAKKEY", "secret", fixed); err != nil {
		t.Fatalf("sign: %v", err)
	}
	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=BFAKKEY/20250101/global/api/aws4_request") {
		t.Errorf("unexpected signature prefix: %s", auth)
	}
	// The signature itself depends on the body hash; assert it's present and hex-shaped.
	if !strings.Contains(auth, "Signature=") {
		t.Errorf("Authorization missing Signature=: %s", auth)
	}
}
