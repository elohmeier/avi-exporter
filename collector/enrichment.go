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
