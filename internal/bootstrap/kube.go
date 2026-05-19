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

// desiredSecret renders the K8s Secret containing the connector's credentials.
// The Bifrost API rotates these on every EnsureConnector call, so the
// controller writes a fresh Secret each time it bootstraps.
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
			"ENROLLMENT_JWT":            creds.EnrollmentJWT,
			"INGRESSIVE_INSTANCE_LABEL": instanceLabel,
		},
	}
}

// desiredDeployment renders the connector Deployment. We use envFrom so the
// container picks up the full Secret as environment variables — that way
// rotating the Secret only requires a pod restart, not a Deployment update.
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
					Containers: []corev1.Container{{
						Name:  "connector",
						Image: image,
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
							},
						}},
						// POD_NAME doubles as the connector's instance label, so each
						// replica's identity in the console is its pod name.
						Env: []corev1.EnvVar{{
							Name: "INGRESSIVE_INSTANCE_LABEL",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.name",
								},
							},
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
// client's ConnectorCredentials struct. We carry our own copy so kube.go has
// no dependency on internal/api (and importantly to keep the import graph
// flowing in the right direction for the future API-client extraction).
type connectorCreds struct {
	AccessKeyID     string
	AccessKeySecret string
	EnrollmentJWT   string
}

// secretsEqual reports whether the relevant fields of two Secrets match. We
// compare just StringData + the labels we set; ignore server-managed fields
// like resourceVersion or unrelated annotations the operator may have added.
func secretsEqual(a, b *corev1.Secret) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !mapEqualString(a.StringData, b.StringData) {
		return false
	}
	// StringData is one-way: when read back from the API server it's empty and
	// the values appear in Data (base64-decoded). Compare both directions.
	if !stringMatchesData(a.StringData, b.Data) && !stringMatchesData(b.StringData, a.Data) {
		// If both have only Data (round-tripped) we compare those.
		if a.StringData == nil && b.StringData == nil {
			if !mapEqualBytes(a.Data, b.Data) {
				return false
			}
		}
	}
	return mapEqualString(a.Labels, b.Labels)
}

// stringMatchesData compares a string map (desired) against a base64-decoded
// byte map (observed). Used to recognize a no-op reconcile when the server
// returns Data even though we sent StringData.
func stringMatchesData(want map[string]string, got map[string][]byte) bool {
	if len(want) == 0 && len(got) == 0 {
		return true
	}
	if len(want) != len(got) {
		return false
	}
	for k, v := range want {
		if string(got[k]) != v {
			return false
		}
	}
	return true
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
	return false, ""
}
