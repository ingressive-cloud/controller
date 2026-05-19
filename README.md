# Ingressive Kubernetes Ingress Controller

A Kubernetes Ingress Controller that exposes services through the [Ingressive](https://ingressive.cloud)
edge network. The controller installs into your cluster, watches `Ingress`
resources with `spec.ingressClassName: ingressive`, and materialises them as
Ingressive Sites — no inbound ports required on cluster nodes; traffic flows
over the Ingressive self-optimising mesh network from edge to cluster.

## What it does

- **Bootstrap**: on startup, calls the Ingressive API to identify itself, then
  reconciles a paired Connector pod (Secret + Deployment) into its namespace.
  The connector is the data path that carries traffic from Ingressive's edge
  to your in-cluster Services.
- **Ingress reconciliation**: watches `Ingress`, `Service`, and `IngressClass`
  resources. For each Ingress with our class, walks its `spec.rules` and
  produces a Site on the Ingressive API. Updates and deletions propagate via
  a finalizer.
- **Cooperative merge**: the controller writes the routing fields it owns
  (paths, upstream, optional rewrite/timeouts/body-size from annotations).
  Fields you edit in the Ingressive console — caching, shield, custom
  request/response headers, etc. — are preserved across reconciles.
- **Multi-Ingress merging**: when two Ingresses target the same host with
  non-overlapping paths, the controller merges them. Path conflicts are
  rejected deterministically (older Ingress wins; the rejected one gets a
  Warning event).
- **Security headers default**: new Sites get a sensible default response
  header set (`X-Frame-Options`, `X-Content-Type-Options`, `X-XSS-Protection`,
  `Referrer-Policy`, `Strict-Transport-Security`). Customize freely in the
  console after first deploy — the controller won't overwrite later.

## Quick install

Create the Controller in your [Ingressive console](https://console.ingressive.cloud)
and copy the API credentials shown once on the controller's page. Then:

```bash
kubectl create namespace ingressive-system

kubectl create secret generic ingressive-controller-credentials \
  --namespace ingressive-system \
  --from-literal=INGRESSIVE_API_KEY_ID=<paste from console> \
  --from-literal=INGRESSIVE_API_KEY_SECRET=<paste from console>

helm install ingressive-controller \
  oci://ghcr.io/ingressive-cloud/controller/chart/ingressive-controller \
  --version 0.1.0 \
  --namespace ingressive-system
```

The controller pod starts, checks in with the API, deploys the paired
Connector pod, and begins watching Ingresses.

## Use it

Add `spec.ingressClassName: ingressive` to your Ingress:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: my-app
spec:
  ingressClassName: ingressive
  rules:
    - host: app.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: my-app
                port:
                  number: 80
```

Apply it, watch the controller log it as reconciled, and find the Site in
your Ingressive console. `kubectl get ingress` will show the hostname in the
`ADDRESS` column once the status is written back.

## Supported annotations

The controller maps a small set of common nginx-ingress annotations. Anything
else lives in the Ingressive console — edit there, the controller won't touch
it on the next reconcile.

| Annotation | Effect |
|---|---|
| `nginx.ingress.kubernetes.io/rewrite-target` | `/` → strip prefix; any other value → replace prefix with that value |
| `nginx.ingress.kubernetes.io/proxy-body-size` | Sets `client_max_body_size` (e.g. `50m`) |
| `nginx.ingress.kubernetes.io/proxy-read-timeout` | Read timeout (bare integer = seconds) |
| `nginx.ingress.kubernetes.io/proxy-send-timeout` | Send timeout |
| `nginx.ingress.kubernetes.io/proxy-connect-timeout` | Connect timeout |
| `ingressive.cloud/security-headers` | `false` disables the default security-headers bundle |

If an annotation is set, the controller will keep that field set on every
reconcile. If it's absent, the controller leaves the field alone — your
console edits stick.

## Configuration

All settings accept a flag or an environment variable.

| Flag | Env | Default | Required |
|---|---|---|---|
| `--api-url` | `INGRESSIVE_API_URL` | `https://console.ingressive.cloud` | no |
| `--api-key-id` | `INGRESSIVE_API_KEY_ID` | — | **yes** |
| `--api-key-secret` | `INGRESSIVE_API_KEY_SECRET` | — | **yes** |
| `--connector-image` | `CONNECTOR_IMAGE` | `ghcr.io/ingressive-cloud/connector:latest` | no |
| `--namespace` | `POD_NAMESPACE` | autodetected | no |
| `--ingress-class` | `INGRESS_CLASS` | `ingressive` | no |

On startup, the Controller will connect to the Ingressive API and configure itself accordingly. There is intentionally little configuration required. 

## Helm chart values

The chart at [`helm/ingressive-controller`](helm/ingressive-controller) exposes:

| Value | Default | Notes |
|---|---|---|
| `image.repository` | `ghcr.io/ingressive-cloud/controller` | |
| `image.tag` | chart's `appVersion` | |
| `image.pullPolicy` | `IfNotPresent` | Use `Never` for kind/local testing |
| `api.url` | `https://console.ingressive.cloud` | |
| `api.secretName` | `ingressive-controller-credentials` | The secret you `kubectl create` |
| `connectorImage.repository` | `ghcr.io/ingressive-cloud/connector` | |
| `connectorImage.tag` | `latest` | |
| `ingressClass.create` | `true` | Set false if you manage IngressClass externally |
| `ingressClass.name` | `ingressive` | Value users put in `spec.ingressClassName` |
| `ingressClass.controller` | `ingressive.cloud/controller` | The controller identifier |
| `resources` | small defaults | Tweak as needed |
| `imagePullSecrets` | `[]` | |
| `podLabels` / `podAnnotations` | `{}` | |

For kind/local development with locally-built images, use the bundled
[`values-dev.yaml`](helm/ingressive-controller/values-dev.yaml).

## Local Development

We have decided to keep the Ingressive Controller relatively lean. We don't intend to get into "annotation hell" implementing various features. 

We're not sure how well the test cases here will work against the production Ingressive instance. Please open an issue. 

Build the binary:

```bash
go build -ldflags "-X main.Version=0.1.0" ./cmd/controller
```

Build the image:

```bash
docker build --build-arg VERSION=0.1.0 -t ingressive-controller:dev .
```

Run tests:

```bash
go test ./...
```

For installing the dev image into a local kind cluster, see the
[`values-dev.yaml`](helm/ingressive-controller/values-dev.yaml) bundle —
sets `image.pullPolicy: Never` and uses the locally-built tags.

## Repository layout

```
controller/
├── cmd/controller/                  # binary entrypoint + version
├── internal/api/                    # Ingressive API client (self-contained, BFAK-signed)
├── internal/bootstrap/              # paired-Connector Secret + Deployment reconciler
├── internal/config/                 # flag + env parsing
├── internal/translate/              # K8s Ingress → SiteConfiguration
├── internal/reconciler/             # controller-runtime Ingress reconciler + cooperative merge
├── helm/ingressive-controller/      # Helm chart
├── .github/workflows/build.yaml     # CI: tests, image push, chart push on tag
└── Dockerfile
```

The `internal/api/` package has no in-repo dependencies — it can be lifted
into a public Go module without restructuring.

## Versioning

The controller binary reports its version on every API request via the
`User-Agent` header (`Ingressive-Controller/<version>`). The version is
injected at build time:

- `go build -ldflags "-X main.Version=$VERSION" ./cmd/controller`
- `docker build --build-arg VERSION=$VERSION .`

If the ldflag is omitted the binary reports `dev`. Release builds use the
git tag (e.g. `v0.1.0` produces a binary reporting `0.1.0`).

The Helm chart and the binary share a version. Tagging `v0.1.0` triggers CI
to build and push both:

- Image: `ghcr.io/ingressive-cloud/controller:0.1.0`
- Chart: `ghcr.io/ingressive-cloud/controller/chart/ingressive-controller:0.1.0`

## License

Apache 2.0. See [LICENSE](LICENSE).
