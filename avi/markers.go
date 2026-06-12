package avi

import "strings"

// MarkerInfo is the AKO-injected metadata we surface on Prometheus labels.
// All fields are empty strings when AKO did not set the corresponding marker.
type MarkerInfo struct {
	ClusterName  string
	Namespace    string
	ServiceName  string
	IngressName  string
	GatewayName  string
	Host         string
	Path         string
	InfraSetting string
}

// ParseMarkers extracts the AKO marker keys we care about.
// AKO marker keys (see vmware/load-balancer-and-ingress-services-for-kubernetes
// internal/lib/lib.go:GetAllMarkers): "clustername", "Namespace", "ServiceName",
// "IngressName", "GatewayName", "Host", "Path", "InfrasettingName".
func ParseMarkers(ms []Marker) MarkerInfo {
	var mi MarkerInfo
	for _, m := range ms {
		v := strings.Join(m.Values, ",")
		switch m.Key {
		case "clustername":
			mi.ClusterName = v
		case "Namespace":
			mi.Namespace = v
		case "ServiceName":
			mi.ServiceName = v
		case "IngressName":
			mi.IngressName = v
		case "GatewayName":
			mi.GatewayName = v
		case "Host":
			mi.Host = v
		case "Path":
			mi.Path = v
		case "InfrasettingName":
			mi.InfraSetting = v
		}
	}
	return mi
}

// ParseObjectMetadata extracts AKO label data from object markers and fills
// gaps from service_metadata. Markers win because they are explicit AKO labels;
// service_metadata is a compatibility fallback for controller endpoints that
// omit markers from inventory summaries.
func ParseObjectMetadata(ms []Marker, metadata ServiceMetadata) MarkerInfo {
	mi := ParseMarkers(ms)
	return MergeServiceMetadata(mi, metadata)
}

// MergeServiceMetadata fills empty MarkerInfo fields from service_metadata.
func MergeServiceMetadata(mi MarkerInfo, metadata ServiceMetadata) MarkerInfo {
	if mi.Namespace == "" {
		mi.Namespace = metadata.Namespace
	}
	if mi.IngressName == "" {
		mi.IngressName = metadata.IngressName
	}
	if mi.Host == "" && len(metadata.Hostnames) > 0 {
		mi.Host = metadata.Hostnames[0]
	}
	if len(metadata.NamespaceIngressName) > 0 {
		ns, ing := splitNamespacedName(metadata.NamespaceIngressName[0])
		if mi.Namespace == "" {
			mi.Namespace = ns
		}
		if mi.IngressName == "" {
			mi.IngressName = ing
		}
	}
	if len(metadata.NamespaceSvcName) > 0 {
		ns, svc := splitNamespacedName(metadata.NamespaceSvcName[0])
		if mi.Namespace == "" {
			mi.Namespace = ns
		}
		if mi.ServiceName == "" {
			mi.ServiceName = svc
		}
	}
	return mi
}

func splitNamespacedName(value string) (string, string) {
	if value == "" {
		return "", ""
	}
	parts := strings.SplitN(value, "/", 2)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

// IsAKOManaged returns true if the object was created by AKO.
// AKO sets created_by to "ako-<cluster>".
func IsAKOManaged(createdBy string) bool {
	return strings.HasPrefix(createdBy, "ako-")
}

// RefUUID extracts the trailing UUID from an Avi object reference URL like
// "https://controller/api/pool/pool-abc-uuid". Returns the input unchanged if
// no "/" is found.
func RefUUID(ref string) string {
	if ref == "" {
		return ""
	}
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		ref = ref[i+1:]
	}
	if i := strings.Index(ref, "#"); i >= 0 {
		return ref[:i]
	}
	return ref
}
