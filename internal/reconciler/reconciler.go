// Package reconciler contains the controller-runtime reconciler that maps
// Kubernetes Ingress objects onto Bifrost Site configurations.
//
// The reconciler is **fully declarative**: on any reconcile event it
// recomputes the entire desired set of Sites by walking every Ingress with
// our class, merging the per-host contributions and pushing the diff to
// Bifrost. The reconcile signal (req.NamespacedName) is only used for
// finalizer maintenance on the triggering Ingress; the rest of the work
// ignores it.
//
// This shape correctly handles the "two Ingresses share a host" pattern:
// each Ingress contributes a slice of (host, path) pairs, the reconciler
// merges contributions per host, and conflicts are resolved deterministically
// in favour of the older Ingress. controller-runtime's informer cache makes
// the global List a local-memory operation, so recomputing on every event is
// cheap.
//
// One IngressReconciler instance owns:
//   - the translation pipeline (Ingress -> SiteConfiguration), delegated to
//     internal/translate;
//   - the lifecycle wiring (finalizer add/remove, Site PUT/DELETE, allowlist
//     union, status writeback);
//   - the set of host names last pushed to Bifrost (lastSites), used to drive
//     DeleteSite when an Ingress disappears or stops claiming a host.
//
// The connector identity (ID + slug) is supplied at construction time — it
// comes from the bootstrap subsystem's initial reconcile result. The
// reconciler is not aware of the bootstrap loop; it just routes requests
// through whatever connector identifiers it was given.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	apiclient "github.com/ingressive-cloud/controller/internal/api"
	"github.com/ingressive-cloud/controller/internal/translate"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Event reasons stamped onto Ingress objects via the EventRecorder. Each
// reason maps to one transition in the reconcile flow so operators can
// scrub `kubectl describe ingress` to see what happened.
const (
	ReasonSynced              = "Synced"              // Site applied + status updated
	ReasonDeleted             = "Deleted"             // Site torn down, finalizer removed
	ReasonServiceNotFound     = "ServiceNotFound"     // backend Service missing; waiting on Service watch
	ReasonServicePortNotFound = "ServicePortNotFound" // backend Service exists but doesn't expose named port
	ReasonTranslateFailed     = "TranslateFailed"     // unexpected translator error
	ReasonAPIError            = "APIError"            // PutSite / DeleteSite / PutConnectorServices failed
	ReasonPathConflict        = "PathConflict"        // Ingress rejected because another older Ingress already claims the same (host, path)
)

// FinalizerName is stamped on every Ingress the controller takes ownership of.
// Its presence is the signal that we have side-effects (Bifrost Sites + the
// connector allowlist) to clean up before the Ingress disappears.
const FinalizerName = "ingressive.cloud/finalizer"

// IngressReconciler is the controller-runtime reconciler for Ingresses with
// our IngressClass. It is constructed in main.go after the bootstrap result is
// known and then handed to the controller-runtime Manager.
type IngressReconciler struct {
	// Client is the cached, manager-injected controller-runtime client. Set by
	// SetupWithManager via mgr.GetClient().
	client.Client

	// APIClient performs the side-effects on the Ingressive API. SiteAPI is a
	// minimal interface so unit tests can supply a hand-rolled fake.
	APIClient apiclient.SiteAPI

	// IngressClass is the spec.ingressClassName value we filter on. Ingresses
	// without this class are ignored.
	IngressClass string

	// ConnectorID is the paired connector's UUID, stamped into every
	// SiteConfiguration's upstream so traffic flows through the connector.
	ConnectorID string

	// ConnectorSlug is the URL-safe identifier used by the allowlist endpoint
	// (PUT /connectors/{slug}/services).
	ConnectorSlug string

	// Log is the structured logger; the per-reconcile logger derives from it.
	Log logr.Logger

	// Recorder fires events onto the Ingress so operators see reconcile
	// outcomes via `kubectl describe ingress` without trawling controller
	// logs. Nil-safe: emit() short-circuits when unset (covers unit tests
	// that don't supply one).
	Recorder record.EventRecorder
}

// SetupWithManager registers the reconciler with the controller-runtime
// Manager. It watches Ingresses directly, plus Services (to retrigger when a
// previously-missing backend appears) and IngressClasses (to retrigger when
// our class is added/changed cluster-wide).
func (r *IngressReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&networkingv1.Ingress{}).
		Watches(
			&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.ingressesForService),
		).
		Watches(
			&networkingv1.IngressClass{},
			handler.EnqueueRequestsFromMapFunc(r.ingressesForIngressClass),
		).
		Named("ingress").
		Complete(r)
}

// rejection records an Ingress dropped from the desired state because an
// older Ingress already claims one of its (host, path) pairs. The whole
// Ingress is rejected on a single conflict — see computeDesired for the
// rationale.
type rejection struct {
	Ingress      types.NamespacedName
	ConflictHost string
	ConflictPath string
	WinnerKey    types.NamespacedName
}

// skip records an Ingress dropped from the desired state because its
// translation failed in a way the reconciler treats as soft (the Service
// watch will retrigger). Reason is the underlying translate error so a
// future event-emission pass can surface it.
type skip struct {
	Ingress types.NamespacedName
	Reason  error
}

// Reconcile is the core loop. The work is fully declarative: every event
// triggers a global recompute over all managed Ingresses. req is only used to
// maintain the finalizer on the triggering object.
func (r *IngressReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("ingress", req.NamespacedName.String())

	// Step 1: fetch the triggering Ingress. NotFound is fine — the global
	// recompute below catches up regardless. We only need the object for
	// finalizer maintenance.
	var ing networkingv1.Ingress
	getErr := r.Get(ctx, req.NamespacedName, &ing)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return ctrl.Result{}, fmt.Errorf("get ingress: %w", getErr)
	}
	ingExists := getErr == nil

	// Step 2: finalizer maintenance for the triggering Ingress (only if it
	// exists and is in our class). When the Ingress is being deleted we DEFER
	// finalizer removal until *after* the global recompute below — we don't
	// want to drop the finalizer first (letting the object disappear) and
	// then race against the Bifrost-side cleanup.
	managed := ingExists &&
		ing.Spec.IngressClassName != nil &&
		*ing.Spec.IngressClassName == r.IngressClass
	if managed && ing.DeletionTimestamp == nil {
		if !controllerutil.ContainsFinalizer(&ing, FinalizerName) {
			controllerutil.AddFinalizer(&ing, FinalizerName)
			if err := r.Update(ctx, &ing); err != nil {
				return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
			}
			// The Update bumped resourceVersion; the watch will requeue with
			// the latest object. Return immediately and let the next pass do
			// the translation work.
			return ctrl.Result{}, nil
		}
	}

	// Step 3a: list class-matching Ingresses that are still active (not being
	// deleted). These feed the desired-state computation.
	active, err := r.listManagedIngresses(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list active ingresses: %w", err)
	}

	// Step 3b: list ALL class-matching Ingresses, including deleting ones, so
	// we can derive which hosts we've previously claimed from cluster state
	// rather than in-memory. This is what makes "delete the Ingress, Site is
	// removed" robust to controller pod restarts: the deleting Ingress's
	// spec.rules still names the hosts it was contributing, even though the
	// object is on its way out.
	allClass, err := r.listAllClassIngresses(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list all class ingresses: %w", err)
	}

	// Step 4: compute the merged desired state across every active Ingress.
	desired, rejected, skipped := r.computeDesired(ctx, active)

	// Step 5: previously-claimed hosts = union of spec.rules[].host across
	// every class-matching Ingress (including deleting ones). Anything in
	// previously-claimed but not in desired needs DeleteSite.
	//
	// Limitation: if a user edits an active Ingress to *drop* a host without
	// deleting the Ingress, the dropped host vanishes from spec.rules and
	// won't appear in previously-claimed — we'll leak the Site. Tracking that
	// case needs an annotation we write on every reconcile; out of scope here,
	// v1 limitation documented.
	prev := claimedHosts(allClass)
	toDelete := make([]string, 0)
	for host := range prev {
		if _, ok := desired[host]; !ok {
			toDelete = append(toDelete, host)
		}
	}
	sort.Strings(toDelete)

	// Operator-visible summary of what this reconcile pass is about to do.
	// Logged at the start so a single grep across the controller logs gives
	// you the story of any Ingress event without needing per-call traces.
	desiredHosts := make([]string, 0, len(desired))
	for h := range desired {
		desiredHosts = append(desiredHosts, h)
	}
	sort.Strings(desiredHosts)
	log.Info("reconciling",
		"active_ingresses", len(active),
		"desired_hosts", desiredHosts,
		"to_delete", toDelete,
		"rejected", len(rejected),
		"skipped", len(skipped))
	for _, rej := range rejected {
		log.Info("ingress rejected: path conflict",
			"offender", rej.Ingress.String(),
			"winner", rej.WinnerKey.String(),
			"host", rej.ConflictHost,
			"path", rej.ConflictPath)
	}
	for _, sk := range skipped {
		log.Info("ingress skipped: translation deferred",
			"ingress", sk.Ingress.String(),
			"reason", sk.Reason.Error())
	}

	// Step 6: widen the connector's allowlist BEFORE the Sites go live. If we
	// PutSite first, Bifrost would start proxying requests to a URL the
	// connector hasn't been told to accept yet → 403 from the connector
	// until the next reconcile. Allowlist-first means by the time edge nginx
	// routes a request, the connector already permits the backend.
	union := allowlistUnion(desired)
	if err := r.APIClient.PutConnectorServices(ctx, r.ConnectorSlug, union); err != nil {
		log.Error(err, "failed to push connector allowlist")
		return ctrl.Result{}, fmt.Errorf("push allowlist: %w", err)
	}
	if len(union) > 0 {
		log.Info("connector allowlist updated", "urls", union)
	}

	// Step 7: apply Sites. Create+update first, in deterministic (sorted) host
	// order so test assertions are stable. Then deletes.
	//
	// Each host follows the cooperative-reconcile flow: GET the current
	// SiteConfiguration, merge translator output over it (preserving any
	// user-edited fields the controller doesn't own — see merge.go), then PUT
	// only when the merge result differs from current. Suppressing no-op PUTs
	// avoids spurious version bumps in Bifrost.
	for _, host := range desiredHosts {
		current, err := r.APIClient.GetSite(ctx, host)
		if err != nil {
			log.Error(err, "failed to get site", "host", host)
			return ctrl.Result{}, fmt.Errorf("get site %q: %w", host, err)
		}
		cfg := desired[host]
		merged := mergeForConnector(current, &cfg, r.ConnectorID)
		if siteConfigEqual(current, merged) {
			log.V(1).Info("site unchanged, skipping put", "host", host)
			continue
		}
		if err := r.APIClient.PutSite(ctx, host, *merged); err != nil {
			log.Error(err, "failed to put site", "host", host)
			return ctrl.Result{}, fmt.Errorf("put site %q: %w", host, err)
		}
		if current == nil {
			log.Info("site created", "host", host, "locations", len(merged.Locations))
		} else {
			log.Info("site updated", "host", host, "locations", len(merged.Locations))
		}
	}
	for _, host := range toDelete {
		if err := r.APIClient.DeleteSite(ctx, host); err != nil {
			log.Error(err, "failed to delete site", "host", host)
			return ctrl.Result{}, fmt.Errorf("delete site %q: %w", host, err)
		}
		log.Info("site deleted", "host", host)
	}

	// Step 8: status writeback. We need to know which Ingresses were accepted
	// vs rejected/skipped — accepted ones get their LoadBalancer.Ingress
	// populated; rejected/skipped ones have any stale status cleared so
	// kubectl doesn't keep showing an address for an Ingress that's not
	// actually serving traffic.
	acceptedKeys := acceptedSet(active, rejected, skipped)
	for i := range active {
		cur := &active[i]
		key := keyOf(cur)
		if _, accepted := acceptedKeys[key]; accepted {
			if err := r.writeStatus(ctx, cur); err != nil {
				return ctrl.Result{}, fmt.Errorf("write status %s: %w", key, err)
			}
		} else {
			// Rejected or skipped: clear any leftover status from a prior
			// reconcile when this Ingress *was* accepted. Otherwise the
			// ADDRESS column lies.
			if err := r.clearStatus(ctx, cur); err != nil {
				return ctrl.Result{}, fmt.Errorf("clear status %s: %w", key, err)
			}
		}
	}

	// Step 9: nothing to persist — previously-claimed hosts come from cluster
	// state on every reconcile (see claimedHosts). lastSites is gone.

	// Step 10: if the triggering Ingress was being deleted, remove its
	// finalizer now that the global recompute has already torn down whatever
	// it was contributing. The class filter in listManagedIngresses already
	// excluded it (Deletion is not by itself disqualifying, but the recompute
	// above won't re-add Sites for an Ingress whose hosts are no longer
	// claimed by anyone — see "Ingress changes class away from ours mid-
	// flight" comment near the class filter for the limitation).
	if managed && ing.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(&ing, FinalizerName) {
		log.Info("ingress deleting; removing finalizer post-recompute")
		controllerutil.RemoveFinalizer(&ing, FinalizerName)
		if err := r.Update(ctx, &ing); err != nil {
			// Cache-lag race: a previous reconcile already removed the
			// finalizer server-side and the API server has since deleted the
			// Ingress. Our informer cache hadn't caught up, so r.Get above
			// returned the stale "finalizer still present" copy. The desired
			// end state — Ingress gone — is already true; treat as success.
			if apierrors.IsNotFound(err) {
				log.V(1).Info("ingress already removed by an earlier reconcile; skipping finalizer update")
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
		}
		// TODO(events): emit Normal/Deleted for the removed object.
	}

	// TODO(events): emit Normal/Synced on accepted Ingresses,
	// Warning/PathConflict on rejected ones (with offending host/path +
	// winner key), Warning/ServiceNotFound on skipped ones.
	_ = rejected
	_ = skipped

	return ctrl.Result{}, nil
}

// computeDesired walks the supplied set of managed Ingresses in deterministic
// order — oldest creationTimestamp first, then namespace, then name — and
// returns:
//   - merged: host -> SiteConfiguration with locations concatenated across
//     every accepted contributor.
//   - rejected: Ingresses excluded because they would claim a (host, path)
//     already claimed by an older Ingress. An Ingress is rejected as a whole
//     on a single conflict: contributions to non-conflicting hosts are
//     dropped too. This forces the admin to resolve the conflict rather than
//     letting half the Ingress serve silently.
//   - skipped: Ingresses excluded because translation returned a soft failure
//     (Service missing / port missing). The Service watch will retrigger us
//     when the dependency appears.
func (r *IngressReconciler) computeDesired(
	ctx context.Context, ingresses []networkingv1.Ingress,
) (map[string]apiclient.SiteConfiguration, []rejection, []skip) {
	// Deterministic order: oldest first, ties broken by namespace then name.
	sort.SliceStable(ingresses, func(i, j int) bool {
		ti := ingresses[i].CreationTimestamp
		tj := ingresses[j].CreationTimestamp
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		if ingresses[i].Namespace != ingresses[j].Namespace {
			return ingresses[i].Namespace < ingresses[j].Namespace
		}
		return ingresses[i].Name < ingresses[j].Name
	})

	merged := map[string]apiclient.SiteConfiguration{}
	// claimed: host -> path -> owning Ingress. Path uniqueness within a host
	// is enforced across the entire managed set, not per-Ingress.
	claimed := map[string]map[string]types.NamespacedName{}
	var rejected []rejection
	var skipped []skip

	translateLog := slogFromLogr(r.Log)

	for i := range ingresses {
		ing := &ingresses[i]
		key := keyOf(ing)

		sites, err := translate.Translate(ctx, ing, r.ConnectorID, r.serviceFetcher(), translateLog.With("ingress", key.String()))
		if err != nil {
			// Soft failure: missing Service / port. The Service watcher
			// retriggers us when the dependency materialises. Anything else
			// also treated as a skip to keep one bad Ingress from blocking
			// the whole reconcile — a future events pass will surface
			// unexpected translate errors as Warning/TranslateFailed.
			var nfErr *translate.ServiceNotFoundError
			var pnErr *translate.PortNotFoundError
			if errors.As(err, &nfErr) || errors.As(err, &pnErr) {
				skipped = append(skipped, skip{Ingress: key, Reason: err})
				continue
			}
			skipped = append(skipped, skip{Ingress: key, Reason: err})
			continue
		}

		// Check for any conflicting (host, path) against earlier claims
		// before committing any of this Ingress's contributions.
		conflict := false
		var (
			conflictHost string
			conflictPath string
			winner       types.NamespacedName
		)
		// Deterministic iteration over hosts so the *reported* conflict is
		// stable when an Ingress has multiple conflicts.
		hosts := make([]string, 0, len(sites))
		for h := range sites {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)
	outer:
		for _, host := range hosts {
			cfg := sites[host]
			slots, ok := claimed[host]
			if !ok {
				continue
			}
			// Locations within a single Ingress are typically already in spec
			// order; sort defensively so the first conflict reported is
			// deterministic across runs.
			locs := make([]apiclient.SiteLocation, len(cfg.Locations))
			copy(locs, cfg.Locations)
			sort.SliceStable(locs, func(a, b int) bool { return locs[a].Path < locs[b].Path })
			for _, loc := range locs {
				if w, exists := slots[loc.Path]; exists {
					conflict = true
					conflictHost = host
					conflictPath = loc.Path
					winner = w
					break outer
				}
			}
		}
		if conflict {
			rejected = append(rejected, rejection{
				Ingress:      key,
				ConflictHost: conflictHost,
				ConflictPath: conflictPath,
				WinnerKey:    winner,
			})
			continue
		}

		// Accept: merge contributions into the global state and mark paths
		// claimed.
		for host, cfg := range sites {
			if _, ok := claimed[host]; !ok {
				claimed[host] = map[string]types.NamespacedName{}
			}
			for _, loc := range cfg.Locations {
				claimed[host][loc.Path] = key
			}
			existing, ok := merged[host]
			if !ok {
				merged[host] = cfg
			} else {
				existing.Locations = append(existing.Locations, cfg.Locations...)
				merged[host] = existing
			}
		}
	}

	return merged, rejected, skipped
}

// acceptedSet returns the set of Ingress keys that contributed to the desired
// state. Built by subtracting rejected/skipped from the full managed set.
func acceptedSet(
	ingresses []networkingv1.Ingress,
	rejected []rejection,
	skipped []skip,
) map[types.NamespacedName]struct{} {
	excl := map[types.NamespacedName]struct{}{}
	for _, r := range rejected {
		excl[r.Ingress] = struct{}{}
	}
	for _, s := range skipped {
		excl[s.Ingress] = struct{}{}
	}
	out := map[types.NamespacedName]struct{}{}
	for i := range ingresses {
		key := keyOf(&ingresses[i])
		if _, e := excl[key]; e {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

// listManagedIngresses lists every Ingress and filters to ones bound to our
// class AND not being deleted. The class filter is applied here rather than
// via a label selector because spec.ingressClassName is a spec field, not a
// label, and the informer doesn't index on it. Deleting Ingresses (those
// with a non-nil DeletionTimestamp) are excluded because their hosts should
// vanish from desired so the diff promotes them into DeleteSite calls — the
// finalizer-removal path at the end of Reconcile then lets the API server
// finish removing the object.
//
// v1 limitation: if an Ingress *was* managed by us (has our finalizer) but
// its class is changed away mid-flight, we no longer see it here and the
// finalizer is stuck until someone deletes the Ingress. Acceptable for v1 —
// we don't engineer around it.
func (r *IngressReconciler) listManagedIngresses(ctx context.Context) ([]networkingv1.Ingress, error) {
	all, err := r.listAllClassIngresses(ctx)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, ing := range all {
		if ing.DeletionTimestamp != nil {
			continue
		}
		out = append(out, ing)
	}
	return out, nil
}

// listAllClassIngresses returns every Ingress in the cluster bound to our
// class, including ones being deleted. Used to derive the previously-claimed
// host set from cluster state (see claimedHosts).
func (r *IngressReconciler) listAllClassIngresses(ctx context.Context) ([]networkingv1.Ingress, error) {
	var list networkingv1.IngressList
	if err := r.List(ctx, &list); err != nil {
		return nil, err
	}
	out := make([]networkingv1.Ingress, 0, len(list.Items))
	for i := range list.Items {
		ing := list.Items[i]
		if ing.Spec.IngressClassName == nil || *ing.Spec.IngressClassName != r.IngressClass {
			continue
		}
		out = append(out, ing)
	}
	return out, nil
}

// claimedHosts returns the union of spec.rules[].host across every Ingress in
// the supplied slice. Empty hosts are ignored. Used to drive DeleteSite calls
// in a way that survives controller pod restarts: in-memory tracking would
// reset on restart and miss "I deleted an Ingress after the controller was
// rebuilt" cleanups; cluster state is the durable source.
func claimedHosts(ingresses []networkingv1.Ingress) map[string]struct{} {
	out := make(map[string]struct{})
	for i := range ingresses {
		for _, rule := range ingresses[i].Spec.Rules {
			if rule.Host == "" {
				continue
			}
			out[rule.Host] = struct{}{}
		}
	}
	return out
}

// keyOf is a small adapter for converting an Ingress object to the
// types.NamespacedName we use as a stable map key.
func keyOf(ing *networkingv1.Ingress) types.NamespacedName {
	return types.NamespacedName{Namespace: ing.Namespace, Name: ing.Name}
}

// allowlistUnion flattens every accepted Ingress's upstream service URLs into
// a sorted, deduplicated slice. Returned as []string so the API client emits
// `[]` (not `null`) when no Ingresses are active.
func allowlistUnion(desired map[string]apiclient.SiteConfiguration) []string {
	seen := map[string]struct{}{}
	for _, cfg := range desired {
		for _, loc := range cfg.Locations {
			if loc.Upstream.Service == "" {
				continue
			}
			seen[loc.Upstream.Service] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// emit records an event on the Ingress, no-op when Recorder is nil so unit
// tests don't have to supply one.
func (r *IngressReconciler) emit(ing *networkingv1.Ingress, eventType, reason, format string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(ing, eventType, reason, format, args...)
}

// serviceFetcher returns a translate.ServiceFetcher closure that resolves
// Services through the manager-injected cached client. The fetcher is
// constructed per-Reconcile so it captures the right context-bound Get
// behavior.
func (r *IngressReconciler) serviceFetcher() translate.ServiceFetcher {
	return func(ctx context.Context, namespace, name string) (*corev1.Service, error) {
		var svc corev1.Service
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &svc); err != nil {
			return nil, err
		}
		return &svc, nil
	}
}

// writeStatus reconciles the Ingress's status.loadBalancer.ingress slice to
// one entry per rule host. We always set Hostname (not IP) because the actual
// edge fleet is round-robin'd at DNS time — there isn't a single IP to
// surface. Skips the API write if the current value already matches.
func (r *IngressReconciler) writeStatus(ctx context.Context, ing *networkingv1.Ingress) error {
	desired := desiredStatusHostnames(ing)
	if statusHostnamesEqual(ing.Status.LoadBalancer.Ingress, desired) {
		return nil
	}
	ing.Status.LoadBalancer.Ingress = desired
	return r.Status().Update(ctx, ing)
}

// clearStatus drops any leftover LoadBalancer.Ingress entries from a managed
// Ingress that is currently rejected or skipped. Keeping the entries would
// make kubectl show an ADDRESS that doesn't correspond to live traffic.
func (r *IngressReconciler) clearStatus(ctx context.Context, ing *networkingv1.Ingress) error {
	if len(ing.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}
	ing.Status.LoadBalancer.Ingress = nil
	return r.Status().Update(ctx, ing)
}

// desiredStatusHostnames builds the slice of LoadBalancerIngress entries the
// status should hold. Entries are returned in the order rules appear; we don't
// sort because Ingress consumers may treat the first entry as primary.
func desiredStatusHostnames(ing *networkingv1.Ingress) []networkingv1.IngressLoadBalancerIngress {
	if len(ing.Spec.Rules) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ing.Spec.Rules))
	out := make([]networkingv1.IngressLoadBalancerIngress, 0, len(ing.Spec.Rules))
	for _, rule := range ing.Spec.Rules {
		if rule.Host == "" {
			continue
		}
		if _, ok := seen[rule.Host]; ok {
			continue
		}
		seen[rule.Host] = struct{}{}
		out = append(out, networkingv1.IngressLoadBalancerIngress{Hostname: rule.Host})
	}
	return out
}

// statusHostnamesEqual compares two LoadBalancerIngress slices element-wise on
// the Hostname field — the only field this controller writes. Order matters:
// if a user (or another controller) shuffles entries we'll rewrite them in our
// canonical order.
func statusHostnamesEqual(a, b []networkingv1.IngressLoadBalancerIngress) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Hostname != b[i].Hostname {
			return false
		}
	}
	return true
}

// ingressesForService is the watch map function that fans Service events out
// to the Ingresses that reference them. Only same-namespace references are
// supported — Kubernetes Ingresses can't reference Services in other
// namespaces, so we don't bother scanning cluster-wide.
func (r *IngressReconciler) ingressesForService(ctx context.Context, obj client.Object) []reconcile.Request {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return nil
	}
	var list networkingv1.IngressList
	if err := r.List(ctx, &list, client.InNamespace(svc.Namespace)); err != nil {
		// We can't enqueue if the list fails. The Ingress will be re-reconciled
		// on its own watch when something else changes; logging here is
		// noisy and not load-bearing.
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		ing := &list.Items[i]
		if !ingressReferencesService(ing, svc.Name) {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: ing.Namespace, Name: ing.Name,
		}})
	}
	return out
}

// ingressReferencesService is a cheap predicate over an Ingress's backend
// references. Used by the Service watch to filter the candidate list.
func ingressReferencesService(ing *networkingv1.Ingress, serviceName string) bool {
	for _, rule := range ing.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, p := range rule.HTTP.Paths {
			if p.Backend.Service != nil && p.Backend.Service.Name == serviceName {
				return true
			}
		}
	}
	return false
}

// ingressesForIngressClass enqueues every Ingress that references our class
// when the IngressClass resource itself changes. Rare but possible (operator
// renames the controller identifier mid-flight), so we cover it.
func (r *IngressReconciler) ingressesForIngressClass(ctx context.Context, obj client.Object) []reconcile.Request {
	ic, ok := obj.(*networkingv1.IngressClass)
	if !ok {
		return nil
	}
	// Only fan out if the affected IngressClass is the one we manage; events
	// for other classes don't change our world.
	if ic.Name != r.IngressClass {
		return nil
	}
	var list networkingv1.IngressList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		ing := &list.Items[i]
		if ing.Spec.IngressClassName == nil || *ing.Spec.IngressClassName != r.IngressClass {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: ing.Namespace, Name: ing.Name,
		}})
	}
	return out
}

// slogFromLogr returns the slog.Logger we pass into the translate package,
// which is slog-native. logr.Logger doesn't directly expose its sink as a
// slog.Handler, so for now we delegate to slog.Default() (which main.go
// initializes at startup, so the output remains unified). The logr argument
// is retained so a future adapter can preserve per-reconcile fields.
func slogFromLogr(l logr.Logger) *slog.Logger {
	_ = l
	return slog.Default()
}
