package reconciler

import (
	"reflect"
	"sort"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
)

// mergeForConnector produces the SiteConfiguration to PUT for a host. It
// implements the cooperative-reconcile semantics described in the brief:
// user-edited fields in the Bifrost console survive a controller reconcile.
//
// Ownership signal: a Location in `current` is considered controller-owned
// iff its Upstream.ConnectorID matches connectorID. Locations pointing at a
// different connector or at a direct backend belong to the user and are
// passed through untouched, in every field, forever.
//
// Branches:
//
//   - Fresh create (current == nil): return a clone of `desired`. The
//     translator's output is exactly what we want on a fresh Site, including
//     security headers, since there's no user state to preserve.
//
//   - Update (current != nil): walk current.Locations, partition by ownership.
//     For each owned location whose path matches one in desired, overlay
//     routing fields from desired and overlay annotation-driven fields only
//     when the translator produced a non-empty value (annotation present).
//     User-managed fields (response_headers, request_headers, edge_cache_*,
//     shield_id) are left untouched. Owned locations at paths no longer in
//     desired are dropped. Unowned locations are passed through. Desired
//     paths not currently owned are added as new owned locations with the
//     full translator field set.
//
// The result's Version is left as the zero value — Bifrost manages versioning
// server-side. Translator-provided Version is intentionally ignored.
func mergeForConnector(current, desired *apiclient.SiteConfiguration, connectorID string) *apiclient.SiteConfiguration {
	if desired == nil {
		// Caller error: there is no desired state to merge. Return current
		// unchanged so the reconciler can decide what to do (in practice the
		// reconciler always passes a non-nil desired; defensive only).
		if current == nil {
			return nil
		}
		out := cloneSiteConfig(current)
		return out
	}

	if current == nil {
		// Fresh create: translator output is authoritative. Clone so the
		// caller can mutate the result without aliasing into `desired`.
		return cloneSiteConfig(desired)
	}

	// Index desired locations by path for lookup during the walk over current.
	desiredByPath := make(map[string]apiclient.SiteLocation, len(desired.Locations))
	for _, loc := range desired.Locations {
		desiredByPath[loc.Path] = loc
	}

	merged := make([]apiclient.SiteLocation, 0, len(current.Locations)+len(desired.Locations))
	// Track which desired paths we've already consumed so we can append the
	// remainder (new paths the controller wants to claim) afterwards.
	consumed := make(map[string]struct{}, len(desired.Locations))

	for _, existing := range current.Locations {
		if existing.Upstream.ConnectorID != connectorID {
			// Unowned: pass through entirely untouched.
			merged = append(merged, cloneLocation(existing))
			continue
		}
		// Owned location. If the translator still claims this path, overlay
		// routing fields. Otherwise drop it — the Ingress no longer wants it.
		dl, want := desiredByPath[existing.Path]
		if !want {
			continue
		}
		merged = append(merged, overlayRouting(existing, dl))
		consumed[existing.Path] = struct{}{}
	}

	// Append desired paths the controller wants to claim that weren't already
	// represented in current. These are wholly translator-driven (the
	// fresh-create branch for a single Location).
	for _, dl := range desired.Locations {
		if _, ok := consumed[dl.Path]; ok {
			continue
		}
		merged = append(merged, cloneLocation(dl))
	}

	return &apiclient.SiteConfiguration{Locations: merged}
}

// overlayRouting applies controller-owned routing + annotation-driven fields
// from desired onto an existing owned location. The existing location's
// user-managed fields (response_headers, request_headers, edge_cache_*,
// shield_id) are preserved verbatim.
//
// Annotation-driven scalar rule: copy from desired only when desired's value
// is non-empty (translator produces "" when the annotation is absent).
// Consequence — "override-or-leave": if the user removes an annotation from
// the Ingress after a prior reconcile set it, the previous value stays in
// place. We don't have a signal to distinguish "annotation removed" from
// "annotation never set", and clobbering would surprise users who edited the
// field in the console.
//
// RewriteTo follows RewriteMode: we only update RewriteTo when RewriteMode is
// present, because Bifrost interprets RewriteTo only in the context of a
// non-empty RewriteMode. If the user drops the rewrite-target annotation but
// leaves rewrite-mode, the previous RewriteTo is overwritten with desired's
// (possibly empty) value — that's the translator's intended pairing.
func overlayRouting(existing, desired apiclient.SiteLocation) apiclient.SiteLocation {
	out := cloneLocation(existing)

	// Routing fields: always overwrite from translator. The Ingress is the
	// source of truth for what backend the controller routes to.
	out.LocationType = desired.LocationType
	out.Path = desired.Path
	out.Upstream = desired.Upstream

	// Annotation-driven fields: overwrite only when annotation present
	// (translator produces non-empty value).
	if desired.RewriteMode != "" {
		out.RewriteMode = desired.RewriteMode
		// RewriteTo only makes sense paired with RewriteMode. Translator
		// always emits them together (empty when no rewrite annotation).
		out.RewriteTo = desired.RewriteTo
	}
	if desired.ClientMaxBodySize != "" {
		out.ClientMaxBodySize = desired.ClientMaxBodySize
	}
	if desired.ProxyReadTimeout != "" {
		out.ProxyReadTimeout = desired.ProxyReadTimeout
	}
	if desired.ProxySendTimeout != "" {
		out.ProxySendTimeout = desired.ProxySendTimeout
	}
	if desired.ProxyConnectTimeout != "" {
		out.ProxyConnectTimeout = desired.ProxyConnectTimeout
	}

	// response_headers, request_headers, edge_cache_*, shield_id are
	// deliberately not touched on update. They belong to the user — either
	// they edited them in the console, or they were the translator's
	// fresh-create defaults and the user has since adopted them.
	// `out` already carries `existing`'s values for these.

	return out
}

// cloneSiteConfig deep-copies a SiteConfiguration so the caller and producer
// don't share location maps. Cheap and obviates a class of "did I just
// mutate a cached value?" bugs.
func cloneSiteConfig(in *apiclient.SiteConfiguration) *apiclient.SiteConfiguration {
	if in == nil {
		return nil
	}
	out := &apiclient.SiteConfiguration{
		Version:   in.Version,
		Locations: make([]apiclient.SiteLocation, len(in.Locations)),
	}
	for i, loc := range in.Locations {
		out.Locations[i] = cloneLocation(loc)
	}
	return out
}

// cloneLocation deep-copies a SiteLocation, allocating fresh maps so callers
// can mutate them without aliasing.
func cloneLocation(in apiclient.SiteLocation) apiclient.SiteLocation {
	out := in
	if in.ResponseHeaders != nil {
		out.ResponseHeaders = make(map[string]string, len(in.ResponseHeaders))
		for k, v := range in.ResponseHeaders {
			out.ResponseHeaders[k] = v
		}
	}
	if in.RequestHeaders != nil {
		out.RequestHeaders = make(map[string]string, len(in.RequestHeaders))
		for k, v := range in.RequestHeaders {
			out.RequestHeaders[k] = v
		}
	}
	if in.EdgeCacheEnabled != nil {
		b := *in.EdgeCacheEnabled
		out.EdgeCacheEnabled = &b
	}
	return out
}

// siteConfigEqual reports whether two SiteConfigurations would PUT
// identically. Used to suppress no-op PUTs and the version bumps they'd
// produce on the server.
//
// Both-nil is treated as equal (shouldn't occur in practice — the reconciler
// always passes a desired). When one is nil and the other is not, they are
// unequal so the caller treats it as needing a PUT. Otherwise locations are
// compared in path-sorted order for a deterministic result independent of
// translator output order. Version is intentionally NOT compared — the
// server manages it and the controller doesn't set it on either side.
func siteConfigEqual(a, b *apiclient.SiteConfiguration) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.Locations) != len(b.Locations) {
		return false
	}
	aLocs := sortedLocations(a.Locations)
	bLocs := sortedLocations(b.Locations)
	for i := range aLocs {
		if !locationEqual(aLocs[i], bLocs[i]) {
			return false
		}
	}
	return true
}

// sortedLocations returns a copy of the input sorted by Path.
func sortedLocations(in []apiclient.SiteLocation) []apiclient.SiteLocation {
	out := make([]apiclient.SiteLocation, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// locationEqual compares two SiteLocations across every field the controller
// or user might care about. Uses reflect.DeepEqual for the maps and pointer
// (EdgeCacheEnabled) to avoid hand-rolling map equality.
func locationEqual(a, b apiclient.SiteLocation) bool {
	if a.LocationType != b.LocationType ||
		a.Path != b.Path ||
		a.Upstream != b.Upstream ||
		a.EdgeCacheTTL != b.EdgeCacheTTL ||
		a.ShieldID != b.ShieldID ||
		a.RewriteMode != b.RewriteMode ||
		a.RewriteTo != b.RewriteTo ||
		a.ClientMaxBodySize != b.ClientMaxBodySize ||
		a.ProxyReadTimeout != b.ProxyReadTimeout ||
		a.ProxySendTimeout != b.ProxySendTimeout ||
		a.ProxyConnectTimeout != b.ProxyConnectTimeout {
		return false
	}
	if !mapEqual(a.ResponseHeaders, b.ResponseHeaders) {
		return false
	}
	if !mapEqual(a.RequestHeaders, b.RequestHeaders) {
		return false
	}
	if !ptrBoolEqual(a.EdgeCacheEnabled, b.EdgeCacheEnabled) {
		return false
	}
	return true
}

// mapEqual treats a nil map and an empty map as equal — JSON round-trips
// through the wire collapse the distinction anyway, and the controller has
// no reason to care.
func mapEqual(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// ptrBoolEqual treats nil and *false as distinct (the server distinguishes
// "field absent" from "field present and false" via the omitempty tag).
func ptrBoolEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
