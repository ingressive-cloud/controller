package api

// ConnectorCredentials is the response body returned by
// POST /controllers/{slug}/connector on the Bifrost API. The field names match
// bifrost/api/handlers_controllers.go (handleControllerConnector).
//
// The access key and JWT are shown ONCE — the API rotates them on every call
// to this endpoint, so the controller persists them into a K8s Secret on its
// first successful response.
type ConnectorCredentials struct {
	ConnectorID     string `json:"connector_id"`
	ConnectorSlug   string `json:"connector_slug"`
	ConnectorName   string `json:"connector_name"`
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	EnrollmentJWT   string `json:"enrollment_jwt"`
}

// APIError is returned for non-2xx responses. It implements `error` so callers
// can errors.As-check it to retrieve the status code.
type APIError struct {
	StatusCode int    // HTTP status code returned by the server
	Status     string // HTTP status text
	Body       string // raw response body for diagnostics
	Method     string // HTTP method that triggered the error
	URL        string // request URL
}

func (e *APIError) Error() string {
	if e.Body == "" {
		return e.Method + " " + e.URL + ": " + e.Status
	}
	return e.Method + " " + e.URL + ": " + e.Status + ": " + e.Body
}

// IsClientError reports whether the response was a 4xx.
func (e *APIError) IsClientError() bool { return e.StatusCode >= 400 && e.StatusCode < 500 }

// IsServerError reports whether the response was a 5xx.
func (e *APIError) IsServerError() bool { return e.StatusCode >= 500 && e.StatusCode < 600 }
