package api

// This file mirrors the SiteConfiguration types defined in the Bifrost server
// (pkg/types/types-site.go). They live here, rather than being imported from
// across the repo boundary, so the controller binary stays independent of the
// server's module. Keep the JSON tags in lock-step with the server's canonical
// shape.

// SiteConfiguration is the full desired state for a Site at a single host.
// One configuration upserted via PUT /sites/{host} replaces the previous one;
// the server idempotently versions when the content changes.
type SiteConfiguration struct {
	Locations []SiteLocation `json:"locations"`
	Version   int            `json:"version,omitempty"`
}

// LocationUpstream defines how traffic for a location is routed. The
// controller always sets ConnectorID + Service (we route through the paired
// connector); Backend is left empty.
type LocationUpstream struct {
	Backend     string `json:"backend,omitempty"`
	ConnectorID string `json:"connector_id,omitempty"`
	Service     string `json:"service,omitempty"`
}

// SiteLocation is a single nginx location block. Field semantics match the
// server's struct verbatim — see bifrost/pkg/types/types-site.go for the
// authoritative validation rules. The controller only populates a subset of
// these (routing + annotation-driven fields); the rest are here so the JSON
// shape round-trips cleanly if we ever start consuming GET /sites responses.
type SiteLocation struct {
	LocationType        string            `json:"type"`
	Path                string            `json:"path"`
	Upstream            LocationUpstream  `json:"upstream"`
	ResponseHeaders     map[string]string `json:"response_headers,omitempty"`
	RequestHeaders      map[string]string `json:"request_headers,omitempty"`
	EdgeCacheTTL        string            `json:"edge_cache_ttl,omitempty"`
	EdgeCacheEnabled    *bool             `json:"edge_cache_enabled,omitempty"`
	ShieldID            string            `json:"shield_id,omitempty"`
	RewriteMode         string            `json:"rewrite_mode,omitempty"`
	RewriteTo           string            `json:"rewrite_to,omitempty"`
	ClientMaxBodySize   string            `json:"client_max_body_size,omitempty"`
	ProxyReadTimeout    string            `json:"proxy_read_timeout,omitempty"`
	ProxySendTimeout    string            `json:"proxy_send_timeout,omitempty"`
	ProxyConnectTimeout string            `json:"proxy_connect_timeout,omitempty"`
}
