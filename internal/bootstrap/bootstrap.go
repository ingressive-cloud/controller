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

// Reconcile is one full pass: check-in with the API to discover our identity
// and report our version, then ensure-connector against the API using the
// returned slug, then reconcile the Secret and Deployment in-cluster.
// Idempotent and safe to call repeatedly. Returns the result for callers that
// need the identifiers; the result is also cached on the struct for retrieval
// via LastResult.
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

	creds, err := b.API.EnsureConnector(ctx, info.Controller.Slug)
	if err != nil {
		return nil, fmt.Errorf("ensure connector on server: %w", err)
	}
	if creds.ConnectorSlug == "" {
		return nil, errors.New("server returned empty connector slug")
	}

	// Owner reference lets us tie connector resources back to the controller's
	// own Deployment, so a `kubectl delete deploy ingressive-controller` also
	// cleans up the connector. Best-effort: silently skip if we can't resolve.
	owner := b.resolveOwnerReference(ctx)

	secret := desiredSecret(
		b.Cfg.Namespace,
		creds.ConnectorSlug,
		b.Cfg.APIURL,
		instanceLabelFor(creds.ConnectorSlug),
		connectorCreds{
			AccessKeyID:     creds.AccessKeyID,
			AccessKeySecret: creds.AccessKeySecret,
			EnrollmentJWT:   creds.EnrollmentJWT,
		},
	)
	if owner != nil {
		secret.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	if err := b.reconcileSecret(ctx, secret); err != nil {
		return nil, fmt.Errorf("reconcile secret: %w", err)
	}

	deployment := desiredDeployment(b.Cfg.Namespace, creds.ConnectorSlug, b.Cfg.ConnectorImage, owner)
	if err := b.reconcileDeployment(ctx, deployment); err != nil {
		return nil, fmt.Errorf("reconcile deployment: %w", err)
	}

	b.log().Info("Connector pod reconciled",
		"connector_slug", creds.ConnectorSlug,
		"namespace", b.Cfg.Namespace,
	)
	result := &BootstrapResult{
		ControllerSlug: info.Controller.Slug,
		ConnectorID:    creds.ConnectorID,
		ConnectorSlug:  creds.ConnectorSlug,
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
	got.Data = nil // force StringData to take effect on update
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
