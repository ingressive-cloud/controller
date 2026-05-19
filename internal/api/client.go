package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// userAgentPrefix is prepended to the version in the User-Agent header. The
// Bifrost API parses this token to record the controller's installed version.
const userAgentPrefix = "Ingressive-Controller"

// defaultTimeout is the per-request timeout when the caller doesn't supply a
// custom http.Client.
const defaultTimeout = 30 * time.Second

// Client is a thin HTTP client for the Ingressive API. It is safe for
// concurrent use.
type Client struct {
	BaseURL    string // e.g. https://console.ingressive.cloud
	KeyID      string // BFAK access key ID
	KeySecret  string // raw base64 access key secret
	Version    string // controller binary version, surfaced via User-Agent
	HTTPClient *http.Client
}

// New constructs a Client. baseURL is the API root; if empty the call will
// fail at first request. version is the controller binary version; pass "dev"
// in development.
func New(baseURL, keyID, keySecret, version string) *Client {
	return &Client{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		KeyID:     keyID,
		KeySecret: keySecret,
		Version:   version,
		HTTPClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// WithHTTPClient lets callers swap the HTTP client (for tests, custom
// transports, or different timeouts).
func (c *Client) WithHTTPClient(hc *http.Client) *Client {
	c.HTTPClient = hc
	return c
}

func (c *Client) userAgent() string {
	v := c.Version
	if v == "" {
		v = "dev"
	}
	return userAgentPrefix + "/" + v
}

// do builds, signs, and issues a request. method/path describe the API
// endpoint (path must start with "/"). reqBody is JSON-encoded; out, if
// non-nil, is JSON-decoded from the response. Returns *APIError for HTTP
// 4xx/5xx and the wrapped underlying error for network/transport failures.
func (c *Client) do(ctx context.Context, method, path string, reqBody, out any) error {
	if c.BaseURL == "" {
		return fmt.Errorf("api client: BaseURL is empty")
	}

	var body io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		body = strings.NewReader(string(b))
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", c.userAgent())

	if c.KeyID != "" {
		if err := signRequest(req, c.KeyID, c.KeySecret); err != nil {
			return fmt.Errorf("sign request: %w", err)
		}
	}

	hc := c.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(raw),
			Method:     method,
			URL:        c.BaseURL + path,
		}
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
