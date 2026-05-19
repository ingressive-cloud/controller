// Package bootstrap reconciles the controller's paired Connector into the
// surrounding Kubernetes cluster.
//
// On startup the controller calls the Ingressive API to materialize (or
// rotate) credentials for its paired connector, then writes a Secret and a
// Deployment that runs the connector binary. The reconciler re-runs on a
// timer to repair drift (someone deletes the Secret, etc.). Reconciliation
// against an Ingress resource lives in a later milestone — this package is
// scoped to the connector bootstrap.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
	"github.com/ingressive-cloud/controller/internal/config"

	"github.com/ingressive-cloud/connector/zitihost"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileInterval is the cadence of the post-bootstrap drift loop. Short
// enough to recover from accidental deletes within a few minutes; long enough
// that the API doesn't see steady-state traffic.
const reconcileInterval = 5 * time.Minute

// Bootstrapper owns the bootstrap reconcile loop. Construct with New and call
// Run.
type Bootstrapper struct {
	API    *apiclient.Client
	Cfg    *config.Config
	Client ctrlclient.Client

	// Logger; defaults to slog.Default().
	Logger *slog.Logger

	// Interval lets tests pick a shorter reconcile interval. Zero means
	// reconcileInterval.
	Interval time.Duration

	// Enroll exchanges a Ziti enrollment JWT for identity.json bytes. Defaults
	// to zitihost.Enroll, which calls the real Ziti controller. Tests inject a
	// stub so they don't need a live Ziti controller to run.
	Enroll func(jwt string) ([]byte, error)

	// lastResult caches the most recent successful BootstrapResult. Read via
	// LastResult.
	lastResult *BootstrapResult
}

// New constructs a Bootstrapper with a default logger.
func New(api *apiclient.Client, cfg *config.Config, kc ctrlclient.Client) *Bootstrapper {
	return &Bootstrapper{
		API:    api,
		Cfg:    cfg,
		Client: kc,
		Logger: slog.Default(),
		Enroll: zitihost.Enroll,
	}
}

func (b *Bootstrapper) interval() time.Duration {
	if b.Interval > 0 {
		return b.Interval
	}
	return reconcileInterval
}

func (b *Bootstrapper) log() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}
	return slog.Default()
}

// BootstrapResult is the small bundle of identifiers a successful reconcile
// produces. The main binary needs these to wire up the Ingress reconciler
// (connector ID and slug feed Site upstreams and the allowlist endpoint;
// controller slug is informational at startup).
type BootstrapResult struct {
	ControllerSlug string
	ConnectorID    string
	ConnectorSlug  string
}

// Run runs the bootstrap once, then re-runs it every interval until ctx is
// cancelled. The first reconcile must succeed; subsequent failures are logged
// but do not cause Run to exit (the controller stays up to retry).
//
// Run is preserved for backwards compatibility (and tests that don't need the
// result). New callers should split via RunInitial + RunDriftLoop so they can
// observe the bootstrap result before starting Ingress reconciliation.
func (b *Bootstrapper) Run(ctx context.Context) error {
	if _, err := b.RunInitial(ctx); err != nil {
		return fmt.Errorf("initial bootstrap: %w", err)
	}
	b.RunDriftLoop(ctx)
	return nil
}

// RunInitial is the synchronous first pass. Returns the BootstrapResult so the
// caller can hand the connector identifiers to downstream subsystems (the
// Ingress reconciler in particular). The result is also cached on the struct
// and retrievable via LastResult.
func (b *Bootstrapper) RunInitial(ctx context.Context) (*BootstrapResult, error) {
	return b.Reconcile(ctx)
}

// RunDriftLoop blocks until ctx is cancelled, re-running Reconcile every
// interval. Failures during the loop are logged but not returned — the
// controller is expected to stay up across transient API hiccups.
func (b *Bootstrapper) RunDriftLoop(ctx context.Context) {
	t := time.NewTicker(b.interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := b.Reconcile(ctx); err != nil {
				b.log().Warn("reconcile failed; will retry", "err", err)
			}
		}
	}
}

// LastResult returns the most recent successful BootstrapResult, or nil if no
// reconcile has succeeded yet. Used by tests; main reads it from RunInitial.
func (b *Bootstrapper) LastResult() *BootstrapResult {
	return b.lastResult
}

// Reconcile is one full pass: check-in with the API, ensure the K8s Secret
// and Deployment match the controller's desired state.
//
// EnsureConnector — which rotates the connector's Ziti identity server-side
// and invalidates any running connector pod — is called only when credentials
// don't already exist locally:
//
//   - First-time bootstrap: no PairedConnector in the check-in response.
//   - K8s Secret missing or missing identity.json: somebody deleted it (or it
//     was minted by a pre-migration controller that stored only the JWT); we
//     need to re-mint and re-enroll.
//
// When EnsureConnector is called the controller also runs Ziti enrollment
// immediately, server-side: the one-time JWT is exchanged for a permanent
// identity.json and stored in the Secret. The connector pod then mounts
// identity.json directly and never enrolls itself, so pod restarts (OOM,
// node drain, image bumps) are trivial — re-read the file, reconnect.
//
// Identity rotation is therefore explicit (delete the Secret to force it)
// rather than ambient. Drift reconciles after the initial successful
// bootstrap don't touch the Ziti controller at all.
func (b *Bootstrapper) Reconcile(ctx context.Context) (*BootstrapResult, error) {
	info, err := b.API.CheckIn(ctx)
	if err != nil {
		return nil, fmt.Errorf("check in with server: %w", err)
	}
	if info.Controller.Slug == "" {
		return nil, errors.New("server returned empty controller slug from check-in")
	}
	b.log().Info("Checked in with server",
		"controller_slug", info.Controller.Slug,
		"controller_status", info.Controller.Status,
	)

	// Owner reference lets us tie connector resources back to the controller's
	// own Deployment, so a `kubectl delete deploy ingressive-controller` also
	// cleans up the connector. Best-effort: silently skip if we can't resolve.
	owner := b.resolveOwnerReference(ctx)

	// Decide whether we need to mint fresh credentials.
	var connectorID, connectorSlug string
	needsCredentials := info.PairedConnector == nil
	if !needsCredentials {
		connectorID = info.PairedConnector.ID
		connectorSlug = info.PairedConnector.Slug
		// Server says a paired connector exists. Verify we have its K8s Secret
		// — including the enrolled identity.json key — in place. If the
		// Secret is missing entirely, or was minted by a pre-migration
		// controller that stored only the JWT, the only way to recover is to
		// re-mint and re-enroll.
		var existing corev1.Secret
		err := b.Client.Get(ctx, types.NamespacedName{
			Namespace: b.Cfg.Namespace,
			Name:      resourceNameFor(connectorSlug),
		}, &existing)
		switch {
		case kerrors.IsNotFound(err):
			b.log().Info("K8s Secret missing; will re-mint credentials",
				"connector_slug", connectorSlug)
			needsCredentials = true
		case err != nil:
			return nil, fmt.Errorf("get connector secret: %w", err)
		default:
			if len(existing.Data[identitySecretKey]) == 0 {
				b.log().Info("K8s Secret missing identity.json; will re-mint credentials",
					"connector_slug", connectorSlug)
				needsCredentials = true
			}
		}
	}

	if needsCredentials {
		creds, err := b.API.EnsureConnector(ctx, info.Controller.Slug)
		if err != nil {
			return nil, fmt.Errorf("ensure connector on server: %w", err)
		}
		if creds.ConnectorSlug == "" {
			return nil, errors.New("server returned empty connector slug")
		}
		connectorID = creds.ConnectorID
		connectorSlug = creds.ConnectorSlug

		// Run Ziti enrollment server-side, here in the controller, so the
		// connector binary never sees the one-time JWT. Failure here returns
		// without writing a partial Secret — the controller will exit, K8s
		// restarts it, and the next attempt gets a fresh JWT from the API.
		identityJSON, err := b.Enroll(creds.EnrollmentJWT)
		if err != nil {
			return nil, fmt.Errorf("enroll connector identity: %w", err)
		}

		secret := desiredSecret(
			b.Cfg.Namespace,
			connectorSlug,
			b.Cfg.APIURL,
			instanceLabelFor(connectorSlug),
			connectorCreds{
				AccessKeyID:     creds.AccessKeyID,
				AccessKeySecret: creds.AccessKeySecret,
				IdentityJSON:    identityJSON,
			},
		)
		if owner != nil {
			secret.OwnerReferences = []metav1.OwnerReference{*owner}
		}
		if err := b.reconcileSecret(ctx, secret); err != nil {
			return nil, fmt.Errorf("reconcile secret: %w", err)
		}
	} else {
		b.log().Debug("connector credentials in place; skipping EnsureConnector",
			"connector_slug", connectorSlug)
	}

	deployment := desiredDeployment(b.Cfg.Namespace, connectorSlug, b.Cfg.ConnectorImage, owner)
	if err := b.reconcileDeployment(ctx, deployment); err != nil {
		return nil, fmt.Errorf("reconcile deployment: %w", err)
	}

	b.log().Info("Connector pod reconciled",
		"connector_slug", connectorSlug,
		"namespace", b.Cfg.Namespace,
	)
	result := &BootstrapResult{
		ControllerSlug: info.Controller.Slug,
		ConnectorID:    connectorID,
		ConnectorSlug:  connectorSlug,
	}
	b.lastResult = result
	return result, nil
}

// reconcileSecret creates the Secret if it doesn't exist; updates it in place
// if the data differs from desired.
func (b *Bootstrapper) reconcileSecret(ctx context.Context, want *corev1.Secret) error {
	var got corev1.Secret
	key := types.NamespacedName{Namespace: want.Namespace, Name: want.Name}
	if err := b.Client.Get(ctx, key, &got); err != nil {
		if kerrors.IsNotFound(err) {
			return b.Client.Create(ctx, want)
		}
		return err
	}
	if secretsEqual(want, &got) {
		return nil
	}
	got.StringData = want.StringData
	got.Data = want.Data
	got.Labels = want.Labels
	got.Type = want.Type
	if len(want.OwnerReferences) > 0 {
		got.OwnerReferences = want.OwnerReferences
	}
	return b.Client.Update(ctx, &got)
}

// reconcileDeployment is the Deployment equivalent of reconcileSecret. We
// patch the running object rather than replace it so the controller-manager's
// status fields are preserved.
func (b *Bootstrapper) reconcileDeployment(ctx context.Context, want *appsv1.Deployment) error {
	var got appsv1.Deployment
	key := types.NamespacedName{Namespace: want.Namespace, Name: want.Name}
	if err := b.Client.Get(ctx, key, &got); err != nil {
		if kerrors.IsNotFound(err) {
			return b.Client.Create(ctx, want)
		}
		return err
	}
	needs, reason := deploymentNeedsUpdate(&got, want)
	if !needs {
		return nil
	}
	b.log().Info("Updating connector Deployment", "reason", reason)
	got.Spec = want.Spec
	got.Labels = want.Labels
	if len(want.OwnerReferences) > 0 {
		got.OwnerReferences = want.OwnerReferences
	}
	return b.Client.Update(ctx, &got)
}

// resolveOwnerReference returns a metav1.OwnerReference pointing at the
// controller's own Deployment, when discoverable. Used to tie the connector
// objects to the controller's lifecycle. Returns nil if any lookup fails —
// owner references are nice-to-have, never load-bearing.
func (b *Bootstrapper) resolveOwnerReference(ctx context.Context) *metav1.OwnerReference {
	podName := os.Getenv("POD_NAME")
	podNS := b.Cfg.Namespace
	if podName == "" || podNS == "" {
		return nil
	}
	var pod corev1.Pod
	if err := b.Client.Get(ctx, types.NamespacedName{Namespace: podNS, Name: podName}, &pod); err != nil {
		return nil
	}
	// Walk up: Pod -> ReplicaSet -> Deployment. The pod's controller is the RS.
	rsRef := metav1.GetControllerOf(&pod)
	if rsRef == nil || rsRef.Kind != "ReplicaSet" {
		return nil
	}
	var rs appsv1.ReplicaSet
	if err := b.Client.Get(ctx, types.NamespacedName{Namespace: podNS, Name: rsRef.Name}, &rs); err != nil {
		return nil
	}
	depRef := metav1.GetControllerOf(&rs)
	if depRef == nil || depRef.Kind != "Deployment" {
		return nil
	}
	bt := true
	return &metav1.OwnerReference{
		APIVersion:         depRef.APIVersion,
		Kind:               depRef.Kind,
		Name:               depRef.Name,
		UID:                depRef.UID,
		BlockOwnerDeletion: &bt,
		Controller:         &bt,
	}
}

// instanceLabelFor is the value we stamp into the Secret's
// INGRESSIVE_INSTANCE_LABEL key. The Deployment overrides it with the pod
// name via Downward API, so this default is mostly informational.
func instanceLabelFor(connectorSlug string) string {
	return "ingressive-connector-" + connectorSlug
}
