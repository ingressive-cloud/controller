package api

import (
	"context"
	"fmt"
	"strings"
)

// controllerConnectorResponse mirrors the JSON shape produced by
// handleControllerConnector in bifrost/api/handlers_controllers.go.
// We keep this as an internal wire type and flatten it into the public
// ConnectorCredentials struct so the API surface stays stable even if the
// server response shape changes.
type controllerConnectorResponse struct {
	Connector struct {
		ID   string `json:"id"`
		Slug string `json:"slug"`
		Name string `json:"name"`
	} `json:"connector"`
	AccessKey struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	} `json:"access_key"`
	EnrollmentJWT string `json:"enrollment_jwt"`
}

// CheckInResponse is the shape returned by POST /controller/check-in.
//
// The server resolves the caller to a controller via the authenticated
// principal; the response tells the binary which controller it is (slug, name,
// status) and whether bootstrap has already produced a paired connector.
type CheckInResponse struct {
	Controller struct {
		ID            string  `json:"id"`
		Slug          string  `json:"slug"`
		Name          string  `json:"name"`
		Status        string  `json:"status"`
		LastVersion   *string `json:"last_version,omitempty"`
		LastVersionAt *string `json:"last_version_at,omitempty"`
	} `json:"controller"`
	PairedConnector *struct {
		ID   string `json:"id"`
		Slug string `json:"slug"`
	} `json:"paired_connector,omitempty"`
}

// CheckIn calls POST /controller/check-in. Reports the controller's version
// and returns the controller's identity (slug, name, status) plus the paired
// connector reference if one already exists.
//
// The access key (set on the Client) identifies the controller; no slug is
// needed in the request. Returns *APIError for non-2xx responses — in
// particular, a 404 means the credential is valid but not linked to a
// controller, which the caller should surface to operators as a configuration
// problem.
func (c *Client) CheckIn(ctx context.Context) (*CheckInResponse, error) {
	body := struct {
		Version string `json:"version,omitempty"`
	}{Version: c.Version}

	var resp CheckInResponse
	if err := c.do(ctx, "POST", "/controller/check-in", body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// EnsureConnector calls POST /controllers/{slug}/connector. The endpoint is
// idempotent: on first call it creates the paired connector and returns its
// credentials; on subsequent calls it rotates the access key and reissues the
// enrollment JWT, returning the existing connector.
//
// The slug is obtained from CheckIn — see the bootstrap reconcile loop. The
// request body is empty; version reporting lives in CheckIn exclusively.
// Returns *APIError for non-2xx responses.
func (c *Client) EnsureConnector(ctx context.Context, controllerSlug string) (*ConnectorCredentials, error) {
	controllerSlug = strings.TrimSpace(controllerSlug)
	if controllerSlug == "" {
		return nil, fmt.Errorf("EnsureConnector: controllerSlug is empty")
	}

	var raw controllerConnectorResponse
	path := "/controllers/" + controllerSlug + "/connector"
	if err := c.do(ctx, "POST", path, nil, &raw); err != nil {
		return nil, err
	}
	return &ConnectorCredentials{
		ConnectorID:     raw.Connector.ID,
		ConnectorSlug:   raw.Connector.Slug,
		ConnectorName:   raw.Connector.Name,
		AccessKeyID:     raw.AccessKey.ID,
		AccessKeySecret: raw.AccessKey.Secret,
		EnrollmentJWT:   raw.EnrollmentJWT,
	}, nil
}
