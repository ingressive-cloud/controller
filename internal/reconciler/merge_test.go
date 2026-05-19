package reconciler

import (
	"reflect"
	"sort"
	"testing"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
)

// mergeTestConnectorID is the controller's connector UUID for the merge
// tests. Anything else in a location's upstream.connector_id marks it
// as user-owned in the merger.
const mergeTestConnectorID = "ctrl-conn"

// ownedLoc is a small builder for a controller-owned location at the given
// path. All other fields default to zero; tests override fields as needed.
func ownedLoc(path string) apiclient.SiteLocation {
	return apiclient.SiteLocation{
		LocationType: "prefix",
		Path:         path,
		Upstream: apiclient.LocationUpstream{
			ConnectorID: mergeTestConnectorID,
			Service:     "http://svc.default.svc.cluster.local:80",
		},
	}
}

// unownedLoc is a location pointed at a *different* connector — never the
// controller's. The merger must leave these alone in every field.
func unownedLoc(path, otherConnector string) apiclient.SiteLocation {
	return apiclient.SiteLocation{
		LocationType: "prefix",
		Path:         path,
		Upstream: apiclient.LocationUpstream{
			ConnectorID: otherConnector,
			Service:     "http://other.svc.cluster.local:80",
		},
	}
}

// directBackendLoc is an unowned location with a Backend (not a connector).
// Some Bifrost users configure a direct backend; the merger must pass these
// through untouched.
func directBackendLoc(path, backend string) apiclient.SiteLocation {
	return apiclient.SiteLocation{
		LocationType: "prefix",
		Path:         path,
		Upstream: apiclient.LocationUpstream{
			Backend: backend,
		},
	}
}

func locByPath(cfg *apiclient.SiteConfiguration, path string) *apiclient.SiteLocation {
	if cfg == nil {
		return nil
	}
	for i := range cfg.Locations {
		if cfg.Locations[i].Path == path {
			return &cfg.Locations[i]
		}
	}
	return nil
}

func sortedPaths(cfg *apiclient.SiteConfiguration) []string {
	out := make([]string, 0, len(cfg.Locations))
	for _, l := range cfg.Locations {
		out = append(out, l.Path)
	}
	sort.Strings(out)
	return out
}

// TestMergeFreshCreate — current==nil means the Site doesn't yet exist on
// the server. The merger returns the translator output unchanged.
func TestMergeFreshCreate(t *testing.T) {
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ResponseHeaders = map[string]string{"X-Frame-Options": "SAMEORIGIN"}
			return l
		}(),
	}}
	merged := mergeForConnector(nil, desired, mergeTestConnectorID)
	if !siteConfigEqual(desired, merged) {
		t.Errorf("fresh create should yield desired unchanged\nwant: %+v\ngot:  %+v", desired, merged)
	}
	// And it must not alias desired's locations or maps.
	if &desired.Locations[0] == &merged.Locations[0] {
		t.Error("fresh create returned aliased location slice")
	}
	if reflect.ValueOf(desired.Locations[0].ResponseHeaders).Pointer() ==
		reflect.ValueOf(merged.Locations[0].ResponseHeaders).Pointer() {
		t.Error("fresh create returned aliased ResponseHeaders map")
	}
}

// TestMergeRoutingPreservesUserBodySize — current has a user-edited
// ClientMaxBodySize; desired (translator) has none. Merged keeps the user's
// value. This is the headline behavior of the cooperative reconciler.
func TestMergeRoutingPreservesUserBodySize(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ClientMaxBodySize = "50m"
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)

	got := locByPath(merged, "/")
	if got == nil {
		t.Fatal("merged dropped the '/' location")
	}
	if got.ClientMaxBodySize != "50m" {
		t.Errorf("ClientMaxBodySize = %q, want %q (user edit must survive)", got.ClientMaxBodySize, "50m")
	}
}

// TestMergeAnnotationOverridesUser — the annotation is present on the
// Ingress (translator emits non-empty value) and overrides the user's
// previous value.
func TestMergeAnnotationOverridesUser(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ClientMaxBodySize = "50m"
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ClientMaxBodySize = "100m"
			return l
		}(),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/")
	if got.ClientMaxBodySize != "100m" {
		t.Errorf("annotation should win on update: got %q, want %q", got.ClientMaxBodySize, "100m")
	}
}

// TestMergePreservesUserResponseHeaders — current has the security header
// bundle WITH user customizations (X-Frame-Options changed from SAMEORIGIN
// to DENY, plus a user-added X-Custom). Desired carries the translator's
// default security bundle. The merger leaves response_headers alone.
func TestMergePreservesUserResponseHeaders(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ResponseHeaders = map[string]string{
				"X-Frame-Options": "DENY",
				"X-Custom":        "x",
			}
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.ResponseHeaders = map[string]string{
				"X-Frame-Options":           "SAMEORIGIN",
				"Strict-Transport-Security": "max-age=15552000",
			}
			return l
		}(),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/")
	if got.ResponseHeaders["X-Frame-Options"] != "DENY" {
		t.Errorf("user-customized X-Frame-Options must survive, got %q", got.ResponseHeaders["X-Frame-Options"])
	}
	if got.ResponseHeaders["X-Custom"] != "x" {
		t.Errorf("user-added X-Custom must survive, got %q", got.ResponseHeaders["X-Custom"])
	}
	if _, ok := got.ResponseHeaders["Strict-Transport-Security"]; ok {
		t.Errorf("translator's STS must NOT be added on update; user owns response_headers now")
	}
}

// TestMergePreservesUserCacheAndShield — fields the controller never sets
// (edge_cache_enabled, edge_cache_ttl, shield_id, request_headers) survive
// every reconcile.
func TestMergePreservesUserCacheAndShield(t *testing.T) {
	off := false
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/")
			l.EdgeCacheEnabled = &off
			l.EdgeCacheTTL = "10m"
			l.ShieldID = "shield-abc"
			l.RequestHeaders = map[string]string{"X-Forwarded-For": "$remote_addr"}
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/")
	if got.EdgeCacheEnabled == nil || *got.EdgeCacheEnabled != false {
		t.Errorf("EdgeCacheEnabled lost: got %v", got.EdgeCacheEnabled)
	}
	if got.EdgeCacheTTL != "10m" {
		t.Errorf("EdgeCacheTTL lost: %q", got.EdgeCacheTTL)
	}
	if got.ShieldID != "shield-abc" {
		t.Errorf("ShieldID lost: %q", got.ShieldID)
	}
	if got.RequestHeaders["X-Forwarded-For"] != "$remote_addr" {
		t.Errorf("RequestHeaders lost: %v", got.RequestHeaders)
	}
}

// TestMergeUnownedLocationPreserved — a location pointing at a *different*
// connector belongs to the user; the merger must not touch it in any
// field. It coexists with our owned location at a different path.
func TestMergeUnownedLocationPreserved(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		ownedLoc("/api"),
		func() apiclient.SiteLocation {
			l := unownedLoc("/legacy", "other-connector")
			l.ClientMaxBodySize = "200m"
			l.ResponseHeaders = map[string]string{"X-Powered-By": "user"}
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/api")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	if got := sortedPaths(merged); !reflect.DeepEqual(got, []string{"/api", "/legacy"}) {
		t.Fatalf("merged paths = %v, want [/api /legacy]", got)
	}
	other := locByPath(merged, "/legacy")
	if other.Upstream.ConnectorID != "other-connector" {
		t.Errorf("unowned location's connector clobbered: %q", other.Upstream.ConnectorID)
	}
	if other.ClientMaxBodySize != "200m" {
		t.Errorf("unowned location's body size lost: %q", other.ClientMaxBodySize)
	}
	if other.ResponseHeaders["X-Powered-By"] != "user" {
		t.Errorf("unowned location's headers lost: %v", other.ResponseHeaders)
	}
}

// TestMergeUnownedDirectBackendPreserved — direct-backend (non-connector)
// locations are the user's domain. The merger must preserve them entirely.
func TestMergeUnownedDirectBackendPreserved(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		directBackendLoc("/custom", "https://upstream.example.org"),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/api")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	if got := sortedPaths(merged); !reflect.DeepEqual(got, []string{"/api", "/custom"}) {
		t.Fatalf("merged paths = %v", got)
	}
	custom := locByPath(merged, "/custom")
	if custom.Upstream.Backend != "https://upstream.example.org" {
		t.Errorf("direct-backend location's Backend lost: %q", custom.Upstream.Backend)
	}
}

// TestMergeOwnedLocationDropped — an owned location with no matching path
// in desired is removed (the Ingress no longer claims it).
func TestMergeOwnedLocationDropped(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		ownedLoc("/old"),
		ownedLoc("/api"),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/api")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	if got := sortedPaths(merged); !reflect.DeepEqual(got, []string{"/api"}) {
		t.Errorf("owned /old should be dropped, merged paths = %v", got)
	}
}

// TestMergeNewOwnedLocationAdded — a desired path the current config doesn't
// have is added as a fresh owned location.
func TestMergeNewOwnedLocationAdded(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/api")}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		ownedLoc("/api"),
		ownedLoc("/v2"),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	if got := sortedPaths(merged); !reflect.DeepEqual(got, []string{"/api", "/v2"}) {
		t.Errorf("expected both paths in merged, got %v", got)
	}
}

// TestMergeRoutingFieldsAlwaysOverwritten — an owned location's upstream
// Service can drift (e.g. backend Service was renamed in K8s). The merger
// always overwrites routing fields from desired.
func TestMergeRoutingFieldsAlwaysOverwritten(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.Upstream.Service = "http://stale.default.svc.cluster.local:80"
			l.LocationType = "exact" // user-edited or stale
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.Upstream.Service = "http://fresh.default.svc.cluster.local:80"
			return l
		}(),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/api")
	if got.Upstream.Service != "http://fresh.default.svc.cluster.local:80" {
		t.Errorf("Service routing field was not overwritten: %q", got.Upstream.Service)
	}
	if got.LocationType != "prefix" {
		t.Errorf("LocationType should follow translator: got %q", got.LocationType)
	}
}

// TestMergeIngressDroppedRewriteAnnotation — the controller previously set
// rewrite_mode=strip_prefix; the user removed the annotation from the
// Ingress. Translator now emits empty. Per the override-or-leave rule, the
// existing "strip_prefix" stays. (Documented limitation of having no
// "annotation was present and is now absent" signal.)
func TestMergeIngressDroppedRewriteAnnotation(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.RewriteMode = "strip_prefix"
			l.RewriteTo = ""
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/api")}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/api")
	if got.RewriteMode != "strip_prefix" {
		t.Errorf("RewriteMode should be left alone when annotation absent: got %q", got.RewriteMode)
	}
}

// TestMergeRewriteModeOverwritesRewriteTo — when desired has RewriteMode
// set, we also propagate RewriteTo (even if empty, since translator emits
// them as a pair). Verifies the documented pairing.
func TestMergeRewriteModeOverwritesRewriteTo(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.RewriteMode = "strip_prefix"
			l.RewriteTo = "/leftover"
			return l
		}(),
	}}
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.RewriteMode = "replace_prefix"
			l.RewriteTo = "/v2"
			return l
		}(),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/api")
	if got.RewriteMode != "replace_prefix" || got.RewriteTo != "/v2" {
		t.Errorf("rewrite pair not propagated: mode=%q to=%q", got.RewriteMode, got.RewriteTo)
	}
}

// TestMergeProxyTimeoutsOverlayOnlyWhenPresent — proxy_send_timeout and
// proxy_connect_timeout follow the same annotation-driven rule as the
// others. Test all three at once.
func TestMergeProxyTimeoutsOverlayOnlyWhenPresent(t *testing.T) {
	cur := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.ProxyReadTimeout = "60s"
			l.ProxySendTimeout = "60s"
			l.ProxyConnectTimeout = "5s"
			return l
		}(),
	}}
	// Desired sets ProxyReadTimeout only; the others must be left alone.
	desired := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation {
			l := ownedLoc("/api")
			l.ProxyReadTimeout = "120s"
			return l
		}(),
	}}
	merged := mergeForConnector(cur, desired, mergeTestConnectorID)
	got := locByPath(merged, "/api")
	if got.ProxyReadTimeout != "120s" {
		t.Errorf("ProxyReadTimeout = %q, want 120s", got.ProxyReadTimeout)
	}
	if got.ProxySendTimeout != "60s" {
		t.Errorf("ProxySendTimeout should be preserved: got %q", got.ProxySendTimeout)
	}
	if got.ProxyConnectTimeout != "5s" {
		t.Errorf("ProxyConnectTimeout should be preserved: got %q", got.ProxyConnectTimeout)
	}
}

// TestSiteConfigEqual_BothNil — defensive: both nil compares equal.
func TestSiteConfigEqual_BothNil(t *testing.T) {
	if !siteConfigEqual(nil, nil) {
		t.Error("both-nil should be equal")
	}
}

// TestSiteConfigEqual_OneNil — exactly one nil is treated as needing a PUT.
func TestSiteConfigEqual_OneNil(t *testing.T) {
	cfg := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/")}}
	if siteConfigEqual(nil, cfg) {
		t.Error("nil vs non-nil must not be equal")
	}
	if siteConfigEqual(cfg, nil) {
		t.Error("non-nil vs nil must not be equal")
	}
}

// TestSiteConfigEqual_OrderIndependent — locations in different order
// compare equal as long as their (path, fields) sets match.
func TestSiteConfigEqual_OrderIndependent(t *testing.T) {
	a := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/a"), ownedLoc("/b")}}
	b := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{ownedLoc("/b"), ownedLoc("/a")}}
	if !siteConfigEqual(a, b) {
		t.Error("path-set-equal configs should compare equal regardless of order")
	}
}

// TestSiteConfigEqual_DifferentResponseHeaders — sentinel that map content
// is part of the comparison.
func TestSiteConfigEqual_DifferentResponseHeaders(t *testing.T) {
	a := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation { l := ownedLoc("/"); l.ResponseHeaders = map[string]string{"X-A": "1"}; return l }(),
	}}
	b := &apiclient.SiteConfiguration{Locations: []apiclient.SiteLocation{
		func() apiclient.SiteLocation { l := ownedLoc("/"); l.ResponseHeaders = map[string]string{"X-A": "2"}; return l }(),
	}}
	if siteConfigEqual(a, b) {
		t.Error("differing response_headers must compare unequal")
	}
}
