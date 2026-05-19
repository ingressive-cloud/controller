package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// SiteAPI is the subset of the API client used by the Ingress reconciler. It
// exists so the reconciler can be unit-tested with a hand-rolled fake instead
// of a stub HTTP server.
type SiteAPI interface {
	GetSite(ctx context.Context, host string) (*SiteConfiguration, error)
	PutSite(ctx context.Context, host string, cfg SiteConfiguration) error
	DeleteSite(ctx context.Context, host string) error
	PutConnectorServices(ctx context.Context, slug string, urls []string) error
}

// Compile-time check that *Client satisfies SiteAPI.
var _ SiteAPI = (*Client)(nil)

// siteDetailResponse mirrors the relevant bits of bifrost's SiteDetailResponse
// (api/requests.go). We only consume `current_config`, which carries the
// active SiteConfiguration as a base64-encoded JSON blob in `config_json`.
// The other fields (site metadata, prior configs, log) are not relevant to
// the cooperative reconciler so we don't surface them.
type siteDetailResponse struct {
	Current *siteConfigEnvelope `json:"current_config"`
}

// siteConfigEnvelope mirrors models.SiteConfig: the server emits the raw
// JSON SiteConfiguration as a []byte, which marshals as a base64 string in
// JSON. We decode it ourselves into the in-package SiteConfiguration.
type siteConfigEnvelope struct {
	SiteID     string `json:"site_id"`
	Version    int    `json:"version"`
	ConfigJSON []byte `json:"config_json"`
	Status     string `json:"status"`
}

// GetSite fetches the current SiteConfiguration for the given host.
//
// Returns (nil, nil) when the site doesn't exist (server 404), distinguishing
// "fresh create" from "update" for the cooperative reconciler. (nil, nil) is
// also returned when the site exists but has no current config (e.g. it was
// created but never had a config applied), or when the embedded config_json
// is empty — same handling, since we have nothing to merge against.
//
// Non-404 4xx/5xx responses are returned as *APIError; transport errors are
// wrapped per the do() contract.
func (c *Client) GetSite(ctx context.Context, host string) (*SiteConfiguration, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, errors.New("GetSite: host is empty")
	}
	var resp siteDetailResponse
	if err := c.do(ctx, http.MethodGet, "/sites/"+host, nil, &resp); err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	if resp.Current == nil || len(resp.Current.ConfigJSON) == 0 {
		return nil, nil
	}
	var cfg SiteConfiguration
	if err := json.Unmarshal(resp.Current.ConfigJSON, &cfg); err != nil {
		return nil, fmt.Errorf("decode current_config: %w", err)
	}
	return &cfg, nil
}

// PutSite upserts a Site. The request body is { "config": <SiteConfiguration> }
// per UpsertSiteRequest in bifrost/api/requests.go. The server treats 200
// (updated) and 201 (created) as success and emits a no-op when the config is
// content-identical to the current version.
func (c *Client) PutSite(ctx context.Context, host string, cfg SiteConfiguration) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return errors.New("PutSite: host is empty")
	}
	body := struct {
		Config SiteConfiguration `json:"config"`
	}{Config: cfg}
	return c.do(ctx, http.MethodPut, "/sites/"+host, body, nil)
}

// DeleteSite removes a Site. A 404 from the server is treated as success: the
// reconciler only cares about end-state, and "already deleted" matches the
// desired state of "gone".
func (c *Client) DeleteSite(ctx context.Context, host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return errors.New("DeleteSite: host is empty")
	}
	err := c.do(ctx, http.MethodDelete, "/sites/"+host, nil, nil)
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		return nil
	}
	return err
}

// PutConnectorServices replaces the connector's allowlist with the supplied
// URLs. The body is a raw JSON array of URL strings (no envelope object), per
// handleSetConnectorServices in bifrost/api/handlers_connectors.go. nil is
// normalized to an empty slice so we emit `[]`, not `null`.
func (c *Client) PutConnectorServices(ctx context.Context, slug string, urls []string) error {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return fmt.Errorf("PutConnectorServices: slug is empty")
	}
	if urls == nil {
		urls = []string{}
	}
	return c.do(ctx, http.MethodPut, "/connectors/"+slug+"/services", urls, nil)
}
