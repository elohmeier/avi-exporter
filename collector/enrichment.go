package collector

import (
	"context"

	"github.com/elohmeier/avi-exporter/avi"
)

func (e *Exporter) enrichVSInventory(ctx context.Context, tenant string, items []avi.VSInventoryItem) []avi.VSInventoryItem {
	if len(items) == 0 {
		return items
	}
	configs, err := e.client.ListVSConfig(ctx, tenant)
	if err != nil {
		if e.logger != nil {
			e.logger.Debug("VS metadata enrichment skipped", "tenant", tenant, "err", err)
		}
		return items
	}

	byUUID := make(map[string]avi.VSConfig, len(configs))
	for _, cfg := range configs {
		byUUID[cfg.UUID] = cfg
	}
	for i := range items {
		if cfg, ok := byUUID[items[i].Config.UUID]; ok {
			mergeVSConfigMetadata(&items[i].Config, cfg)
		}
	}
	inheritParentVSNamespace(items)
	return items
}

func mergeVSConfigMetadata(dst *avi.VSConfig, src avi.VSConfig) {
	if dst.Name == "" {
		dst.Name = src.Name
	}
	if dst.CreatedBy == "" {
		dst.CreatedBy = src.CreatedBy
	}
	if dst.Type == "" {
		dst.Type = src.Type
	}
	if dst.VHParentVSRef == "" {
		dst.VHParentVSRef = src.VHParentVSRef
	}
	if dst.VsvipRef == "" {
		dst.VsvipRef = src.VsvipRef
	}
	if dst.PoolRef == "" {
		dst.PoolRef = src.PoolRef
	}
	if dst.PoolGroupRef == "" {
		dst.PoolGroupRef = src.PoolGroupRef
	}
	if len(src.Markers) > 0 {
		dst.Markers = src.Markers
	}
	if !src.ServiceMetadata.Empty() {
		dst.ServiceMetadata = src.ServiceMetadata
	}
}

func inheritParentVSNamespace(items []avi.VSInventoryItem) {
	type parentNamespace struct {
		namespace string
		mixed     bool
	}
	namespaces := make(map[string]parentNamespace)
	for _, item := range items {
		parentUUID := avi.RefUUID(item.Config.VHParentVSRef)
		if parentUUID == "" {
			continue
		}
		mi := avi.ParseObjectMetadata(item.Config.Markers, item.Config.ServiceMetadata)
		if mi.Namespace == "" {
			continue
		}
		state := namespaces[parentUUID]
		if state.namespace == "" {
			state.namespace = mi.Namespace
		} else if state.namespace != mi.Namespace {
			state.mixed = true
		}
		namespaces[parentUUID] = state
	}

	for i := range items {
		mi := avi.ParseObjectMetadata(items[i].Config.Markers, items[i].Config.ServiceMetadata)
		if mi.Namespace != "" {
			continue
		}
		state := namespaces[items[i].Config.UUID]
		if state.namespace != "" && !state.mixed {
			items[i].Config.ServiceMetadata.Namespace = state.namespace
		}
	}
}

func enrichVsVipInventory(items []avi.VsVipInventoryItem, vsItems []avi.VSInventoryItem) []avi.VsVipInventoryItem {
	if len(items) == 0 || len(vsItems) == 0 {
		return items
	}

	vsByUUID := make(map[string]avi.VSInventoryItem, len(vsItems))
	childrenByParent := make(map[string][]avi.VSInventoryItem)
	for _, vs := range vsItems {
		if vs.Config.UUID != "" {
			vsByUUID[vs.Config.UUID] = vs
		}
		parentUUID := avi.RefUUID(vs.Config.VHParentVSRef)
		if parentUUID != "" {
			childrenByParent[parentUUID] = append(childrenByParent[parentUUID], vs)
		}
	}

	byVsVip := make(map[string][]avi.VSInventoryItem)
	seenByVsVip := make(map[string]map[string]bool)
	addLinkedVS := func(vsvipUUID string, vs avi.VSInventoryItem) {
		if vsvipUUID == "" || vs.Config.UUID == "" {
			return
		}
		seen := seenByVsVip[vsvipUUID]
		if seen == nil {
			seen = make(map[string]bool)
			seenByVsVip[vsvipUUID] = seen
		}
		if seen[vs.Config.UUID] {
			return
		}
		byVsVip[vsvipUUID] = append(byVsVip[vsvipUUID], vs)
		seen[vs.Config.UUID] = true
	}

	for _, vs := range vsItems {
		vsvipUUID := avi.RefUUID(vs.Config.VsvipRef)
		if vsvipUUID == "" {
			continue
		}
		addLinkedVS(vsvipUUID, vs)
		for _, child := range childrenByParent[vs.Config.UUID] {
			addLinkedVS(vsvipUUID, child)
		}
		for _, childRef := range vs.Runtime.VHChildVsRef {
			if child, ok := vsByUUID[avi.RefUUID(childRef)]; ok {
				addLinkedVS(vsvipUUID, child)
			}
		}
	}

	for i := range items {
		linked := byVsVip[items[i].Config.UUID]
		if len(linked) == 0 {
			continue
		}
		enrichVsVipConfigFromVS(&items[i].Config, linked)
	}
	return items
}

func enrichVsVipConfigFromVS(dst *avi.VsVipConfig, linked []avi.VSInventoryItem) {
	mergeVsVipVirtualServices(dst, linked)
	src := bestVsVipMetadataSource(linked)
	if src != nil {
		if dst.CreatedBy == "" {
			dst.CreatedBy = linkedCreatedBy(linked)
		}
		if len(linked) == 1 {
			if metadataScore(src.Config.Markers, src.Config.ServiceMetadata) > metadataScore(dst.Markers, dst.ServiceMetadata) {
				dst.Markers = src.Config.Markers
			}
			if dst.ServiceMetadata.Empty() && !src.Config.ServiceMetadata.Empty() {
				dst.ServiceMetadata = src.Config.ServiceMetadata
			}
		}
	}
	fillVsVipUniqueMetadata(dst, linked)
}

func linkedCreatedBy(items []avi.VSInventoryItem) string {
	for _, item := range items {
		if avi.IsAKOManaged(item.Config.CreatedBy) {
			return item.Config.CreatedBy
		}
	}
	for _, item := range items {
		if item.Config.CreatedBy != "" {
			return item.Config.CreatedBy
		}
	}
	return ""
}

func mergeVsVipVirtualServices(dst *avi.VsVipConfig, linked []avi.VSInventoryItem) {
	seen := make(map[string]bool, len(dst.VirtualServices)+len(linked))
	for i := range dst.VirtualServices {
		uuid := dst.VirtualServices[i].UUID
		if uuid == "" {
			uuid = avi.RefUUID(dst.VirtualServices[i].Ref)
			dst.VirtualServices[i].UUID = uuid
		}
		if uuid != "" {
			seen[uuid] = true
		}
	}
	for _, vs := range linked {
		if vs.Config.UUID == "" || seen[vs.Config.UUID] {
			continue
		}
		dst.VirtualServices = append(dst.VirtualServices, avi.VsRef{UUID: vs.Config.UUID, Ref: vs.Config.URL})
		seen[vs.Config.UUID] = true
	}
}

func bestVsVipMetadataSource(items []avi.VSInventoryItem) *avi.VSInventoryItem {
	best := -1
	bestScore := -1
	for i := range items {
		score := metadataScore(items[i].Config.Markers, items[i].Config.ServiceMetadata)
		if avi.IsAKOManaged(items[i].Config.CreatedBy) {
			score++
		}
		if score > bestScore {
			best = i
			bestScore = score
		}
	}
	if best < 0 {
		return nil
	}
	return &items[best]
}

func metadataScore(markers []avi.Marker, metadata avi.ServiceMetadata) int {
	mi := avi.ParseObjectMetadata(markers, metadata)
	score := 0
	if mi.ClusterName != "" {
		score++
	}
	if mi.Host != "" {
		score += 2
	}
	if mi.IngressName != "" {
		score += 4
	}
	if mi.ServiceName != "" {
		score += 8
	}
	if mi.Namespace != "" {
		score += 16
	}
	return score
}

func fillVsVipUniqueMetadata(dst *avi.VsVipConfig, linked []avi.VSInventoryItem) {
	mi := avi.ParseObjectMetadata(dst.Markers, dst.ServiceMetadata)

	if mi.Namespace == "" {
		dst.ServiceMetadata.Namespace = uniqueVSMetadataValue(linked, func(mi avi.MarkerInfo) string {
			return mi.Namespace
		})
		mi = avi.ParseObjectMetadata(dst.Markers, dst.ServiceMetadata)
	}
	if mi.Host == "" {
		if host := uniqueVSMetadataValue(linked, func(mi avi.MarkerInfo) string { return mi.Host }); host != "" {
			dst.ServiceMetadata.Hostnames = []string{host}
		}
		mi = avi.ParseObjectMetadata(dst.Markers, dst.ServiceMetadata)
	}
	if mi.ServiceName == "" {
		if service := uniqueVSMetadataValue(linked, func(mi avi.MarkerInfo) string { return mi.ServiceName }); service != "" {
			dst.ServiceMetadata.NamespaceSvcName = []string{namespacedValue(mi.Namespace, service)}
		}
		mi = avi.ParseObjectMetadata(dst.Markers, dst.ServiceMetadata)
	}
	if mi.IngressName == "" {
		if ingress := uniqueVSMetadataValue(linked, func(mi avi.MarkerInfo) string { return mi.IngressName }); ingress != "" {
			dst.ServiceMetadata.NamespaceIngressName = []string{namespacedValue(mi.Namespace, ingress)}
		}
	}
}

func uniqueVSMetadataValue(items []avi.VSInventoryItem, value func(avi.MarkerInfo) string) string {
	var unique string
	for _, item := range items {
		v := value(avi.ParseObjectMetadata(item.Config.Markers, item.Config.ServiceMetadata))
		if v == "" {
			continue
		}
		if unique == "" {
			unique = v
			continue
		}
		if unique != v {
			return ""
		}
	}
	return unique
}

func namespacedValue(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func (e *Exporter) enrichPoolInventory(ctx context.Context, tenant string, items []avi.PoolInventoryItem) []avi.PoolInventoryItem {
	if len(items) == 0 {
		return items
	}
	configs, err := e.client.ListPoolConfig(ctx, tenant)
	if err != nil {
		if e.logger != nil {
			e.logger.Debug("pool metadata enrichment skipped", "tenant", tenant, "err", err)
		}
		return items
	}

	byUUID := make(map[string]avi.PoolConfig, len(configs))
	for _, cfg := range configs {
		byUUID[cfg.UUID] = cfg
	}
	for i := range items {
		if cfg, ok := byUUID[items[i].Config.UUID]; ok {
			mergePoolConfigMetadata(&items[i].Config, cfg)
		}
	}
	return items
}

func mergePoolConfigMetadata(dst *avi.PoolConfig, src avi.PoolConfig) {
	if dst.Name == "" {
		dst.Name = src.Name
	}
	if dst.CreatedBy == "" {
		dst.CreatedBy = src.CreatedBy
	}
	if len(src.Markers) > 0 {
		dst.Markers = src.Markers
	}
	if !src.ServiceMetadata.Empty() {
		dst.ServiceMetadata = src.ServiceMetadata
	}
	if dst.Tier1LR == "" {
		dst.Tier1LR = src.Tier1LR
	}
}
