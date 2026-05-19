// Package translate converts a Kubernetes Ingress into one or more
// Bifrost SiteConfiguration objects.
//
// The translation is a pure function from (Ingress, connector ID, Service
// resolver) to map[host]SiteConfiguration. Callers supply the Service-fetch
// closure so the same code can run against a real cluster (controller-runtime
// client) or a static fixture (tests).
//
// Scope: rule-based routing only. spec.tls and spec.defaultBackend are
// recognized and warned about but not translated — Ingressive auto-issues
// certificates, and we don't ship a per-Ingress default backend in v1.
package translate

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	apiclient "github.com/ingressive-cloud/controller/internal/api"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

// ServiceFetcher resolves a Service reference to its cluster-scoped object.
// Callers can satisfy this with controller-runtime's Get for the real path,
// or a fixture map for tests.
type ServiceFetcher func(ctx context.Context, namespace, name string) (*corev1.Service, error)

// ServiceNotFoundError indicates the Ingress references a Service that does
// not (yet) exist. The reconciler treats this as a soft failure — the Service
// may appear later and the controller-runtime watch will requeue when it does.
type ServiceNotFoundError struct {
	Namespace string
	Name      string
}

func (e *ServiceNotFoundError) Error() string {
	return fmt.Sprintf("service %s/%s not found", e.Namespace, e.Name)
}

// PortNotFoundError indicates the Ingress references a port by name that the
// Service does not expose.
type PortNotFoundError struct {
	Namespace   string
	ServiceName string
	PortName    string
}

func (e *PortNotFoundError) Error() string {
	return fmt.Sprintf("service %s/%s does not expose port %q",
		e.Namespace, e.ServiceName, e.PortName)
}

// Annotation prefix recognized by the translator. Anything inside this prefix
// that we don't explicitly map is logged as a warning.
const (
	annotationPrefix       = "nginx.ingress.kubernetes.io/"
	annSecurityHeaders     = "ingressive.cloud/security-headers"
)

// securityHeaders is the bundle applied when `ingressive.cloud/security-headers`
// is set on an Ingress. Mirrors the "Add Security Headers" option exposed by
// the Bifrost console (see api/handlers_site.go, the SecurityHeaders branch).
// Kept in sync deliberately — divergence would surprise users who toggle
// between the two paths.
var securityHeaders = map[string]string{
	"X-Frame-Options":           "SAMEORIGIN",
	"X-Content-Type-Options":    "nosniff",
	"X-XSS-Protection":          "1; mode=block",
	"Referrer-Policy":           "strict-origin-when-cross-origin",
	"Strict-Transport-Security": "max-age=31536000",
}

// Annotation keys we currently consume. Keep in sync with the docs in README.
const (
	annRewriteTarget       = annotationPrefix + "rewrite-target"
	annProxyBodySize       = annotationPrefix + "proxy-body-size"
	annProxyReadTimeout    = annotationPrefix + "proxy-read-timeout"
	annProxySendTimeout    = annotationPrefix + "proxy-send-timeout"
	annProxyConnectTimeout = annotationPrefix + "proxy-connect-timeout"
	// The following are recognized only so we can emit a more specific warning
	// rather than the generic "unknown annotation" path.
	annConfigurationSnippet = annotationPrefix + "configuration-snippet"
)

// Translate converts ing into one SiteConfiguration per host listed in
// spec.rules. The returned map is keyed by host (the Site ID).
//
// connectorID is the paired connector's UUID; every Upstream points at it.
// serviceFetch resolves Service references; tests pass a fixture closure.
// logger receives warnings about unknown annotations and ignored fields. A nil
// logger falls back to slog.Default().
func Translate(
	ctx context.Context,
	ing *networkingv1.Ingress,
	connectorID string,
	serviceFetch ServiceFetcher,
	logger *slog.Logger,
) (map[string]apiclient.SiteConfiguration, error) {
	if ing == nil {
		return nil, fmt.Errorf("translate: ingress is nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("ingress", ing.Namespace+"/"+ing.Name)

	// Surface out-of-scope spec sections as warnings up-front. We still
	// translate the rest of the Ingress when these are present.
	if len(ing.Spec.TLS) > 0 {
		logger.Warn("spec.tls is ignored; Ingressive auto-issues certificates")
	}
	if ing.Spec.DefaultBackend != nil {
		logger.Warn("spec.defaultBackend is not translated in v1; use spec.rules")
	}

	annFields, err := parseAnnotations(ing.Annotations, logger)
	if err != nil {
		return nil, err
	}

	out := make(map[string]apiclient.SiteConfiguration)
	for _, rule := range ing.Spec.Rules {
		if rule.Host == "" {
			logger.Warn("rule with empty host skipped — Ingressive requires a host per rule")
			continue
		}
		if rule.HTTP == nil || len(rule.HTTP.Paths) == 0 {
			logger.Warn("rule has no HTTP paths; skipped", "host", rule.Host)
			continue
		}

		// A single Ingress may list the same host twice (uncommon but legal).
		// In that case we concatenate the paths so we emit one SiteConfiguration
		// per host.
		cfg := out[rule.Host]
		for _, p := range rule.HTTP.Paths {
			loc, err := buildLocation(ctx, ing.Namespace, p, connectorID, annFields, serviceFetch)
			if err != nil {
				return nil, err
			}
			cfg.Locations = append(cfg.Locations, loc)
		}
		out[rule.Host] = cfg
	}
	return out, nil
}

// annotationFields are the values plucked from an Ingress's annotations,
// applied uniformly to every location we generate from it.
type annotationFields struct {
	RewriteMode         string
	RewriteTo           string
	ClientMaxBodySize   string
	ProxyReadTimeout    string
	ProxySendTimeout    string
	ProxyConnectTimeout string
	SecurityHeaders     bool
}

// parseAnnotations walks ing.Annotations, picks out the keys we recognize, and
// logs a warning for any nginx.ingress.kubernetes.io/* annotation we don't.
func parseAnnotations(annotations map[string]string, logger *slog.Logger) (annotationFields, error) {
	// SecurityHeaders defaults ON. The annotation is opt-OUT: set
	// ingressive.cloud/security-headers: "false" to skip the bundle.
	out := annotationFields{SecurityHeaders: true}
	for k, v := range annotations {
		// Ingressive-namespaced annotations are recognized alongside the
		// nginx.ingress.kubernetes.io/* set.
		if k == annSecurityHeaders {
			out.SecurityHeaders = parseBool(v)
			continue
		}
		if !strings.HasPrefix(k, annotationPrefix) {
			continue
		}
		switch k {
		case annRewriteTarget:
			// "/" → strip prefix. Anything else → replace_prefix with v.
			if v == "/" {
				out.RewriteMode = "strip_prefix"
				out.RewriteTo = ""
			} else {
				out.RewriteMode = "replace_prefix"
				out.RewriteTo = v
			}
		case annProxyBodySize:
			out.ClientMaxBodySize = v
		case annProxyReadTimeout:
			out.ProxyReadTimeout = normalizeTimeout(v)
		case annProxySendTimeout:
			out.ProxySendTimeout = normalizeTimeout(v)
		case annProxyConnectTimeout:
			out.ProxyConnectTimeout = normalizeTimeout(v)
		case annConfigurationSnippet:
			logger.Warn("ignoring configuration-snippet annotation — arbitrary nginx config is not supported",
				"annotation", k)
		default:
			logger.Warn("unknown nginx.ingress.kubernetes.io annotation; ignored",
				"annotation", k, "value", v)
		}
	}
	return out, nil
}

// parseBool accepts the loose set of "truthy" values K8s annotations
// conventionally use (case-insensitive): true, yes, 1, on. Anything else,
// including the empty string, is false.
func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "yes", "1", "on":
		return true
	}
	return false
}

// normalizeTimeout converts the "bare integer means seconds" shorthand used by
// the nginx-ingress annotations into the explicit "<n>s" form Bifrost expects.
// Non-integer values pass through unchanged.
func normalizeTimeout(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return v
	}
	if _, err := strconv.Atoi(v); err == nil {
		return v + "s"
	}
	return v
}

// buildLocation produces a single SiteLocation from an Ingress HTTPIngressPath.
// pathType drives LocationType; the backend reference is resolved to a
// cluster-internal URL via serviceFetch; annotation-derived fields are merged.
func buildLocation(
	ctx context.Context,
	namespace string,
	p networkingv1.HTTPIngressPath,
	connectorID string,
	ann annotationFields,
	serviceFetch ServiceFetcher,
) (apiclient.SiteLocation, error) {
	locType := pathTypeToLocationType(p.PathType)

	path := p.Path
	if path == "" {
		// PathType=ImplementationSpecific without a path is legal in K8s; we
		// default to "/" which is the most permissive thing the validator
		// accepts.
		path = "/"
	}

	url, err := resolveBackendURL(ctx, namespace, p.Backend, serviceFetch)
	if err != nil {
		return apiclient.SiteLocation{}, err
	}

	loc := apiclient.SiteLocation{
		LocationType: locType,
		Path:         path,
		Upstream: apiclient.LocationUpstream{
			ConnectorID: connectorID,
			Service:     url,
		},
		RewriteMode:         ann.RewriteMode,
		RewriteTo:           ann.RewriteTo,
		ClientMaxBodySize:   ann.ClientMaxBodySize,
		ProxyReadTimeout:    ann.ProxyReadTimeout,
		ProxySendTimeout:    ann.ProxySendTimeout,
		ProxyConnectTimeout: ann.ProxyConnectTimeout,
	}
	if ann.SecurityHeaders {
		// Copy so two locations don't end up sharing a map reference.
		loc.ResponseHeaders = make(map[string]string, len(securityHeaders))
		for k, v := range securityHeaders {
			loc.ResponseHeaders[k] = v
		}
	}
	return loc, nil
}

// pathTypeToLocationType maps the Kubernetes pathType enum to Bifrost's
// LocationType string. ImplementationSpecific falls through to "prefix" as the
// most permissive default — matches the behavior of the upstream nginx-ingress
// controller.
func pathTypeToLocationType(pt *networkingv1.PathType) string {
	if pt == nil {
		return "prefix"
	}
	switch *pt {
	case networkingv1.PathTypeExact:
		return "exact"
	case networkingv1.PathTypePrefix:
		return "prefix"
	case networkingv1.PathTypeImplementationSpecific:
		return "prefix"
	default:
		return "prefix"
	}
}

// resolveBackendURL produces the cluster-internal HTTP URL the connector will
// proxy to. K8s Ingress backends reference a Service by name + port, where the
// port may be numeric (port.number) or symbolic (port.name). For symbolic
// ports we have to look up the Service to translate the name to a number.
func resolveBackendURL(
	ctx context.Context,
	namespace string,
	backend networkingv1.IngressBackend,
	serviceFetch ServiceFetcher,
) (string, error) {
	if backend.Service == nil {
		return "", fmt.Errorf("translate: backend.Resource references are not supported; only backend.Service")
	}
	svcRef := backend.Service
	port, err := resolveServicePort(ctx, namespace, svcRef, serviceFetch)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svcRef.Name, namespace, port), nil
}

// resolveServicePort returns the numeric port for a backend Service reference.
// Always fetches the Service to verify it exists — refusing to translate an
// Ingress whose backend has no corresponding Service surfaces the broken state
// cleanly (the reconciler requeues and the Service watch retriggers when the
// Service appears). The alternative — writing a Site pointing at a nonexistent
// upstream — produces "all green, requests fail" which is genuinely hard to
// debug, so we deviate from upstream nginx-ingress-controller behavior here.
// Returns ServiceNotFoundError / PortNotFoundError for the expected failure
// modes so the caller can react accordingly.
func resolveServicePort(
	ctx context.Context,
	namespace string,
	ref *networkingv1.IngressServiceBackend,
	serviceFetch ServiceFetcher,
) (int32, error) {
	if ref.Port.Number == 0 && ref.Port.Name == "" {
		return 0, fmt.Errorf("translate: backend.service.port has neither number nor name")
	}
	svc, err := serviceFetch(ctx, namespace, ref.Name)
	if err != nil {
		return 0, &ServiceNotFoundError{Namespace: namespace, Name: ref.Name}
	}
	if ref.Port.Number != 0 {
		// We don't enforce that the Service declares this exact port — users
		// may legitimately target a port the Service exposes via spec.ports
		// in a way Ingress can't see (e.g. service-mesh sidecars). Existence
		// of the Service is the meaningful check.
		return ref.Port.Number, nil
	}
	// Named port: translate via the Service.
	for _, p := range svc.Spec.Ports {
		if p.Name == ref.Port.Name {
			return p.Port, nil
		}
	}
	return 0, &PortNotFoundError{
		Namespace:   namespace,
		ServiceName: ref.Name,
		PortName:    ref.Port.Name,
	}
}
