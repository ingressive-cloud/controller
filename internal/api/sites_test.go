package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestGetSite_HappyPath — server returns a SiteDetailResponse with
// current_config.config_json encoding a SiteConfiguration. We decode it and
// return the SiteConfiguration to the caller.
func TestGetSite_HappyPath(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
	)
	cfg := SiteConfiguration{Locations: []SiteLocation{{
		LocationType: "prefix",
		Path:         "/api",
		Upstream:     LocationUpstream{ConnectorID: "cn-1", Service: "http://svc:80"},
		ClientMaxBodySize: "50m",
	}}}
	cfgJSON, _ := json.Marshal(cfg)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		// config_json is a []byte in the source struct, which marshals as a
		// base64 string. encoding/json on this server-side struct does that
		// automatically, so we can just embed the raw JSON via the struct.
		body := map[string]any{
			"current_config": map[string]any{
				"site_id":     "app.example.com",
				"version":     3,
				"config_json": cfgJSON, // []byte → base64 JSON string
				"status":      "active",
			},
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	got, err := c.GetSite(context.Background(), "app.example.com")
	if err != nil {
		t.Fatalf("GetSite: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %q", gotMethod)
	}
	if gotPath != "/sites/app.example.com" {
		t.Errorf("path = %q", gotPath)
	}
	if got == nil {
		t.Fatalf("expected non-nil SiteConfiguration")
	}
	if len(got.Locations) != 1 || got.Locations[0].Path != "/api" ||
		got.Locations[0].ClientMaxBodySize != "50m" {
		t.Errorf("config round-trip lost data: %+v", got)
	}
}

// TestGetSite_404IsNil — site doesn't exist → (nil, nil), so the reconciler
// can branch on "fresh create".
func TestGetSite_404IsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"site not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	got, err := c.GetSite(context.Background(), "missing.example.com")
	if err != nil {
		t.Fatalf("404 should not error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil SiteConfiguration on 404, got %+v", got)
	}
}

// TestGetSite_5xxPropagates — non-404 errors surface as *APIError.
func TestGetSite_5xxPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	_, err := c.GetSite(context.Background(), "x.example.com")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 APIError, got %v", err)
	}
}

// TestGetSite_NullCurrent — server returns 200 with current_config: null
// (site row exists but no config yet). Treated as (nil, nil) so the merger
// uses the fresh-create branch.
func TestGetSite_NullCurrent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"current_config":null}`))
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	got, err := c.GetSite(context.Background(), "x.example.com")
	if err != nil {
		t.Fatalf("GetSite: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil on null current_config, got %+v", got)
	}
}

// TestGetSite_EmptyHostRejected — guard.
func TestGetSite_EmptyHostRejected(t *testing.T) {
	c := New("http://example", "k", "s", "test")
	if _, err := c.GetSite(context.Background(), "  "); err == nil {
		t.Error("expected error for empty host")
	}
}

// TestPutSite_HappyPath — request shape, headers, and body match the spec for
// PUT /sites/{host}. The body must be a JSON object with a "config" key.
func TestPutSite_HappyPath(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		gotCT     string
		gotBody   map[string]json.RawMessage
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"site_id":"app.example.com","version":1,"created":true}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "BFAKKEYID", "secret", "test")
	cfg := SiteConfiguration{Locations: []SiteLocation{{
		LocationType: "prefix",
		Path:         "/",
		Upstream:     LocationUpstream{ConnectorID: "cn-1", Service: "http://svc.default.svc.cluster.local:80"},
	}}}
	if err := c.PutSite(context.Background(), "app.example.com", cfg); err != nil {
		t.Fatalf("PutSite: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if gotPath != "/sites/app.example.com" {
		t.Errorf("path = %q", gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q", gotCT)
	}
	// Body must be { "config": {...} } — not the SiteConfiguration directly.
	if _, ok := gotBody["config"]; !ok {
		t.Fatalf("body missing 'config' key: %v", gotBody)
	}
	var cfgOut SiteConfiguration
	if err := json.Unmarshal(gotBody["config"], &cfgOut); err != nil {
		t.Fatalf("decode inner config: %v", err)
	}
	if len(cfgOut.Locations) != 1 || cfgOut.Locations[0].Path != "/" {
		t.Errorf("config round-trip lost data: %+v", cfgOut)
	}
}

// TestPutSite_4xx — server-side validation failure surfaces as *APIError.
func TestPutSite_4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"config validation failed"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	err := c.PutSite(context.Background(), "x.example.com", SiteConfiguration{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", apiErr.StatusCode)
	}
}

// TestPutSite_EmptyHostRejected — guard against accidental empty-host PUTs.
func TestPutSite_EmptyHostRejected(t *testing.T) {
	c := New("http://example", "k", "s", "test")
	if err := c.PutSite(context.Background(), "  ", SiteConfiguration{}); err == nil {
		t.Error("expected error for empty host")
	}
}

// TestDeleteSite_HappyPath — DELETE /sites/{host} with no body.
func TestDeleteSite_HappyPath(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		bodyLen   int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		bodyLen = len(raw)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	if err := c.DeleteSite(context.Background(), "app.example.com"); err != nil {
		t.Fatalf("DeleteSite: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q", gotMethod)
	}
	if gotPath != "/sites/app.example.com" {
		t.Errorf("path = %q", gotPath)
	}
	if bodyLen != 0 {
		t.Errorf("DELETE should not send a body, got %d bytes", bodyLen)
	}
}

// TestDeleteSite_404IsSuccess — already-gone is fine; reconciler should not
// have to special-case it.
func TestDeleteSite_404IsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"site not found"}`, http.StatusNotFound)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	if err := c.DeleteSite(context.Background(), "missing.example.com"); err != nil {
		t.Errorf("404 should be treated as success, got %v", err)
	}
}

// TestDeleteSite_5xxPropagates — server errors must still surface.
func TestDeleteSite_5xxPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	err := c.DeleteSite(context.Background(), "x.example.com")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 APIError, got %v", err)
	}
}

// TestPutConnectorServices_HappyPath — body is a bare JSON array.
func TestPutConnectorServices_HappyPath(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		gotBody   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"services":[]}`))
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	err := c.PutConnectorServices(context.Background(), "my-connector",
		[]string{"http://a.default.svc.cluster.local:80", "http://b.default.svc.cluster.local:8080"})
	if err != nil {
		t.Fatalf("PutConnectorServices: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q", gotMethod)
	}
	if gotPath != "/connectors/my-connector/services" {
		t.Errorf("path = %q", gotPath)
	}
	// Body must decode as a JSON array of strings, not an object.
	var out []string
	if err := json.Unmarshal([]byte(gotBody), &out); err != nil {
		t.Fatalf("body is not a JSON string array: %q", gotBody)
	}
	if len(out) != 2 || out[0] != "http://a.default.svc.cluster.local:80" {
		t.Errorf("body urls round-trip lost: %v", out)
	}
}

// TestPutConnectorServices_NilBecomesEmptyArray — nil must serialize as `[]`,
// not `null`. The server rejects `null` body.
func TestPutConnectorServices_NilBecomesEmptyArray(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	if err := c.PutConnectorServices(context.Background(), "any", nil); err != nil {
		t.Fatalf("PutConnectorServices: %v", err)
	}
	if strings.TrimSpace(gotBody) != "[]" {
		t.Errorf("nil slice should encode as `[]`, got %q", gotBody)
	}
}

// TestPutConnectorServices_4xx — typed APIError on validation failure.
func TestPutConnectorServices_4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad scheme"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := New(srv.URL, "k", "s", "test")
	err := c.PutConnectorServices(context.Background(), "x", []string{"ftp://nope"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 APIError, got %v", err)
	}
}

// TestPutConnectorServices_EmptySlugRejected — guard.
func TestPutConnectorServices_EmptySlugRejected(t *testing.T) {
	c := New("http://example", "k", "s", "test")
	if err := c.PutConnectorServices(context.Background(), "", []string{"http://x"}); err == nil {
		t.Error("expected error for empty slug")
	}
}
