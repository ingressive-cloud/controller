// Command controller runs the Ingressive Kubernetes Ingress Controller. The
// binary has two phases:
//
//  1. Bootstrap — call the Ingressive API to identify this controller, then
//     provision (or reconcile) the paired Connector Deployment + Secret. This
//     runs synchronously at startup so the rest of the binary can take a
//     dependency on the connector identifiers returned. A periodic drift loop
//     re-runs the bootstrap reconcile every few minutes.
//
//  2. Ingress reconciliation — start a controller-runtime Manager that
//     watches Ingresses (and Services + IngressClasses) and materializes them
//     as Sites on Bifrost via the connector. This blocks for the lifetime of
//     the binary.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
	"github.com/ingressive-cloud/controller/internal/bootstrap"
	"github.com/ingressive-cloud/controller/internal/config"
	"github.com/ingressive-cloud/controller/internal/reconciler"

	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// Version is the controller binary version, surfaced to the Ingressive API in
// the User-Agent header. Overridden at build time via
//
//	go build -ldflags "-X main.Version=$VERSION"
var Version = "dev"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Bridge slog → logr so the controller-runtime Manager and the reconciler's
	// logr.Logger calls produce visible output. Without this, controller-runtime
	// warns "log.SetLogger(...) was never called" and all `r.Log.Info(...)`
	// calls silently go nowhere.
	ctrllog.SetLogger(logr.FromSlogHandler(logger.Handler()))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(2)
	}
	logger.Info("Ingressive Controller starting",
		"version", Version,
		"api_url", cfg.APIURL,
		"namespace", cfg.Namespace,
		"ingress_class", cfg.IngressClass,
	)

	api := apiclient.New(cfg.APIURL, cfg.APIKeyID, cfg.APIKeySecret, Version)

	// controller-runtime's GetConfigOrDie() reads in-cluster config when
	// running as a pod, and falls back to ~/.kube/config for local dev.
	restCfg := ctrl.GetConfigOrDie()
	kc, err := client.New(restCfg, client.Options{})
	if err != nil {
		logger.Error("kube client", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Phase 1: bootstrap. RunInitial is synchronous; we need its result
	// (connector ID + slug) before starting the Ingress reconciler.
	bs := bootstrap.New(api, cfg, kc)
	bs.Logger = logger
	result, err := bs.RunInitial(ctx)
	if err != nil {
		logger.Error("initial bootstrap", "err", err)
		os.Exit(1)
	}
	logger.Info("Initial bootstrap complete",
		"controller_slug", result.ControllerSlug,
		"connector_id", result.ConnectorID,
		"connector_slug", result.ConnectorSlug,
	)
	// Drift loop runs for the lifetime of the process; failures inside are
	// logged but never returned.
	go bs.RunDriftLoop(ctx)

	// Phase 2: Manager + Ingress reconciler. We share the manager's scheme
	// (which already covers core + networking v1) so we don't need to
	// register custom types.
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{Scheme: scheme.Scheme})
	if err != nil {
		logger.Error("controller manager", "err", err)
		os.Exit(1)
	}
	ingressReconciler := &reconciler.IngressReconciler{
		Client:        mgr.GetClient(),
		APIClient:     api,
		IngressClass:  cfg.IngressClass,
		ConnectorID:   result.ConnectorID,
		ConnectorSlug: result.ConnectorSlug,
		Log:           ctrl.Log.WithName("ingress-reconciler"),
	}
	if err := ingressReconciler.SetupWithManager(mgr); err != nil {
		logger.Error("setup ingress reconciler", "err", err)
		os.Exit(1)
	}

	logger.Info("Starting controller-runtime manager")
	if err := mgr.Start(ctx); err != nil {
		logger.Error("manager exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("Ingressive Controller stopped")
}
