package bootstrap

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// labelsFor returns the labels we attach to managed objects. They double as
// selector targets for the Deployment.
func labelsFor(connectorSlug string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "ingressive-connector",
		"app.kubernetes.io/managed-by": "ingressive-controller",
		"app.kubernetes.io/instance":   connectorSlug,
	}
}

// resourceNameFor is the Secret/Deployment name for a paired connector. We
// derive it from the slug so multiple controllers in one namespace don't
// collide (in practice there's only one, but cheap insurance).
func resourceNameFor(connectorSlug string) string {
	return "ingressive-connector-" + connectorSlug
}

// identitySecretKey is the key under which the enrolled Ziti identity.json
// bytes live in the connector Secret. The Deployment mounts this key as a
// file at identityMountPath; the connector reads the file on startup.
const identitySecretKey = "identity.json"

// identityMountDir is the directory inside the connector container where the
// Secret's identity.json key is mounted. The Deployment sets
// INGRESSIVE_IDENTITY_DIR to this path so the connector binary reads
// <dir>/identity.json. We use a dedicated path (rather than overlaying
// /etc/ingressive) so the volume mount doesn't clobber anything else the
// container image ships in /etc/ingressive — and so the file mount stays
// read-only without needing subPath gymnastics.
const identityMountDir = "/var/run/ingressive"

// identityVolumeName is the name we give the projected Secret volume that
// exposes identity.json to the connector container.
const identityVolumeName = "ingressive-identity"

// desiredSecret renders the K8s Secret containing the connector's credentials
// and enrolled Ziti identity. The Bifrost API rotates the access key on every
// EnsureConnector call; the controller calls EnsureConnector at most once per
// identity (the resulting identity.json is durable) so this Secret is in
// practice written once and updated only on credential rotation triggered by
// Secret deletion. Scalar env vars live in StringData; the identity.json
// bytes go into Data so the file mount sees the raw JSON unchanged.
func desiredSecret(namespace, connectorSlug, apiURL, instanceLabel string, creds connectorCreds) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceNameFor(connectorSlug),
			Namespace: namespace,
			Labels:    labelsFor(connectorSlug),
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"INGRESSIVE_API_URL":        apiURL,
			"INGRESSIVE_API_KEY_ID":     creds.AccessKeyID,
			"INGRESSIVE_API_KEY_SECRET": creds.AccessKeySecret,
			"INGRESSIVE_INSTANCE_LABEL": instanceLabel,
		},
		Data: map[string][]byte{
			identitySecretKey: creds.IdentityJSON,
		},
	}
}

// desiredDeployment renders the connector Deployment.
//
// The Secret is consumed two ways:
//   - envFrom: scalar credentials (API URL, key id/secret, instance label) are
//     exposed as environment variables. The connector binary reads these via
//     os.Getenv. Rotating these values only requires a pod restart, not a
//     Deployment update.
//   - volume mount: the Secret's identity.json key is projected as a file at
//     /var/run/ingressive/identity.json. INGRESSIVE_IDENTITY_DIR points the
//     connector binary at that directory. Mounting via a dedicated path
//     (rather than overlaying /etc/ingressive) keeps the volume read-only
//     without subPath tricks and avoids clobbering anything the image ships
//     in /etc/ingressive.
//
// Note: a Secret volume projects every key in the Secret as a file under the
// mount directory. The scalar env-var keys would appear as files there too,
// which is harmless — the connector only reads identity.json from this
// directory and ignores anything else.
func desiredDeployment(namespace, connectorSlug, image string, owner *metav1.OwnerReference) *appsv1.Deployment {
	labels := labelsFor(connectorSlug)
	replicas := int32(1)
	secretName := resourceNameFor(connectorSlug)

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceNameFor(connectorSlug),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
					MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 0},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{
						Name: identityVolumeName,
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName: secretName,
								Items: []corev1.KeyToPath{{
									Key:  identitySecretKey,
									Path: identitySecretKey,
								}},
							},
						},
					}},
					Containers: []corev1.Container{{
						Name:  "connector",
						Image: image,
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
							},
						}},
						Env: []corev1.EnvVar{
							// POD_NAME doubles as the connector's instance label, so each
							// replica's identity in the console is its pod name.
							{
								Name: "INGRESSIVE_INSTANCE_LABEL",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.name",
									},
								},
							},
							// Point the connector at the Secret-projected identity.json.
							{
								Name:  "INGRESSIVE_IDENTITY_DIR",
								Value: identityMountDir,
							},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      identityVolumeName,
							MountPath: identityMountDir,
							ReadOnly:  true,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("50m"),
								corev1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					}},
				},
			},
		},
	}
	if owner != nil {
		dep.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	return dep
}

// connectorCreds is the bootstrap package's internal mirror of the API
// client's ConnectorCredentials struct, plus the enrolled identity bytes the
// controller produces in Reconcile. We carry our own copy so kube.go has
// no dependency on internal/api (and importantly to keep the import graph
// flowing in the right direction for the future API-client extraction).
//
// IdentityJSON is the JSON config bytes returned by zitihost.Enroll — the
// permanent Ziti identity, not the one-time JWT. Persisting these into the
// K8s Secret means pod restarts can mount the same identity rather than
// re-enrolling.
type connectorCreds struct {
	AccessKeyID     string
	AccessKeySecret string
	IdentityJSON    []byte
}

// secretsEqual reports whether the relevant fields of two Secrets match.
// Equality is checked on the union of StringData (scalar env vars) and Data
// (binary keys like identity.json) — when a Secret is read back from the
// API server its StringData is empty and every value lives in Data
// (base64-decoded by the client). We normalize both to a single map[string]
// []byte before comparing. Labels are also compared; server-managed fields
// like resourceVersion and unrelated annotations are ignored.
func secretsEqual(a, b *corev1.Secret) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !mapEqualBytes(mergeSecretData(a), mergeSecretData(b)) {
		return false
	}
	return mapEqualString(a.Labels, b.Labels)
}

// mergeSecretData returns a unified view of a Secret's key/value contents,
// folding StringData (string values) and Data (byte values) into one map so
// the equality check doesn't have to reason about both shapes. StringData
// wins on duplicate keys, matching the API server's own resolution rule.
func mergeSecretData(s *corev1.Secret) map[string][]byte {
	out := make(map[string][]byte, len(s.Data)+len(s.StringData))
	for k, v := range s.Data {
		out[k] = v
	}
	for k, v := range s.StringData {
		out[k] = []byte(v)
	}
	return out
}

func mapEqualString(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func mapEqualBytes(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if string(b[k]) != string(v) {
			return false
		}
	}
	return true
}

// deploymentNeedsUpdate reports whether the running Deployment differs from
// the desired spec in a way that matters. We deliberately compare a narrow
// surface: the image, replica count, container env, and labels — everything
// else (status, server-managed fields, kube-controller-manager defaults) is
// ignored. Returns a human-readable reason for logging on a true result.
func deploymentNeedsUpdate(got, want *appsv1.Deployment) (bool, string) {
	if got == nil {
		return true, "deployment absent"
	}
	if len(got.Spec.Template.Spec.Containers) == 0 {
		return true, "no containers"
	}
	wantC := want.Spec.Template.Spec.Containers[0]
	gotC := got.Spec.Template.Spec.Containers[0]
	if gotC.Image != wantC.Image {
		return true, fmt.Sprintf("image %q != %q", gotC.Image, wantC.Image)
	}
	if (got.Spec.Replicas == nil) != (want.Spec.Replicas == nil) {
		return true, "replica nil-ness differs"
	}
	if got.Spec.Replicas != nil && want.Spec.Replicas != nil && *got.Spec.Replicas != *want.Spec.Replicas {
		return true, fmt.Sprintf("replicas %d != %d", *got.Spec.Replicas, *want.Spec.Replicas)
	}
	if !mapEqualString(got.Labels, want.Labels) {
		return true, "labels differ"
	}
	// envFrom changes (secret-name swap) — rare but worth detecting.
	if len(gotC.EnvFrom) != len(wantC.EnvFrom) {
		return true, "envFrom count differs"
	}
	for i := range gotC.EnvFrom {
		if gotC.EnvFrom[i].SecretRef == nil || wantC.EnvFrom[i].SecretRef == nil {
			return true, "envFrom shape differs"
		}
		if gotC.EnvFrom[i].SecretRef.Name != wantC.EnvFrom[i].SecretRef.Name {
			return true, "envFrom secret name differs"
		}
	}
	// Detect the migration from "JWT in env" to "identity.json mounted as a
	// file": volume + volumeMount + INGRESSIVE_IDENTITY_DIR env all need to be
	// present. If a pre-migration Deployment is still running, this triggers
	// the rollout that picks them up.
	if !hasIdentityVolume(got.Spec.Template.Spec.Volumes) {
		return true, "identity volume missing"
	}
	if !hasIdentityVolumeMount(gotC.VolumeMounts) {
		return true, "identity volumeMount missing"
	}
	if !hasIdentityDirEnv(gotC.Env) {
		return true, "INGRESSIVE_IDENTITY_DIR env missing"
	}
	return false, ""
}

func hasIdentityVolume(vols []corev1.Volume) bool {
	for _, v := range vols {
		if v.Name == identityVolumeName && v.Secret != nil {
			return true
		}
	}
	return false
}

func hasIdentityVolumeMount(mounts []corev1.VolumeMount) bool {
	for _, m := range mounts {
		if m.Name == identityVolumeName && m.MountPath == identityMountDir {
			return true
		}
	}
	return false
}

func hasIdentityDirEnv(env []corev1.EnvVar) bool {
	for _, e := range env {
		if e.Name == "INGRESSIVE_IDENTITY_DIR" && e.Value == identityMountDir {
			return true
		}
	}
	return false
}
