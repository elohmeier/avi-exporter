package avi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

const defaultPageSize = 200
const maxPoolRuntimeDetailPages = 100

// listAll walks the page→next chain and appends results into out.
// path is like "/api/vsinventory"; extra query params are merged.
func listAll[T any](ctx context.Context, c *Client, path, tenant string, extra url.Values) ([]T, error) {
	var all []T
	page := 1
	for {
		q := url.Values{}
		for k, v := range extra {
			q[k] = v
		}
		q.Set("page", strconv.Itoa(page))
		q.Set("page_size", strconv.Itoa(defaultPageSize))

		var resp PageResp[T]
		err := c.Get(ctx, path, &resp, RequestOptions{Tenant: tenant, Query: q})
		if err != nil {
			return nil, fmt.Errorf("list %s page %d: %w", path, page, err)
		}
		all = append(all, resp.Results...)
		if resp.Next == nil || *resp.Next == "" {
			break
		}
		page++
	}
	return all, nil
}

// ListTenants returns all tenants visible to the calling user. Requires admin
// privileges and the "*" tenant header (or no tenant header).
func (c *Client) ListTenants(ctx context.Context) ([]Tenant, error) {
	return listAll[Tenant](ctx, c, "/api/tenant", "*", nil)
}

// GetClusterRuntime returns the cluster runtime snapshot.
func (c *Client) GetClusterRuntime(ctx context.Context) (*ClusterRuntime, error) {
	var rt ClusterRuntime
	if err := c.Get(ctx, "/api/cluster/runtime", &rt, RequestOptions{Tenant: "admin"}); err != nil {
		return nil, err
	}
	return &rt, nil
}

// GetCluster returns the cluster configuration.
func (c *Client) GetCluster(ctx context.Context) (*Cluster, error) {
	var cl Cluster
	if err := c.Get(ctx, "/api/cluster", &cl, RequestOptions{Tenant: "admin"}); err != nil {
		return nil, err
	}
	return &cl, nil
}

// inventoryExtra is the standard set of query params for inventory endpoints:
// expand UUID refs and inline the runtime/health_score sub-objects we read.
func inventoryExtra() url.Values {
	q := url.Values{}
	q.Set("include_name", "true")
	q.Set("include", "runtime,health_score")
	return q
}

func fieldsExtra(fields string) url.Values {
	q := url.Values{}
	q.Set("fields", fields)
	return q
}

// ListVSInventory returns all VS inventory entries for the given tenant.
func (c *Client) ListVSInventory(ctx context.Context, tenant string) ([]VSInventoryItem, error) {
	return listAll[VSInventoryItem](ctx, c, "/api/virtualservice-inventory", tenant, inventoryExtra())
}

// ListVSConfig returns the VS config fields needed to restore AKO metadata
// that the inventory endpoint omits on some controller versions.
func (c *Client) ListVSConfig(ctx context.Context, tenant string) ([]VSConfig, error) {
	return listAll[VSConfig](ctx, c, "/api/virtualservice", tenant,
		fieldsExtra("uuid,name,created_by,markers,service_metadata,type,vh_parent_vs_ref,vip,services"))
}

// ListPoolInventory returns all pool inventory entries for the given tenant.
func (c *Client) ListPoolInventory(ctx context.Context, tenant string) ([]PoolInventoryItem, error) {
	return listAll[PoolInventoryItem](ctx, c, "/api/pool-inventory", tenant, inventoryExtra())
}

// ListPoolConfig returns the pool config fields needed to restore AKO metadata
// that the inventory endpoint omits on some controller versions.
func (c *Client) ListPoolConfig(ctx context.Context, tenant string) ([]PoolConfig, error) {
	return listAll[PoolConfig](ctx, c, "/api/pool", tenant,
		fieldsExtra("uuid,name,created_by,markers,service_metadata,tier1_lr"))
}

// ListSEInventory returns all service-engine inventory entries (admin tenant).
func (c *Client) ListSEInventory(ctx context.Context) ([]SEInventoryItem, error) {
	return listAll[SEInventoryItem](ctx, c, "/api/serviceengine-inventory", "admin", inventoryExtra())
}

// ListSEConfig returns controller-owned Service Engine configuration including
// management and data vNIC address assignments.
func (c *Client) ListSEConfig(ctx context.Context) ([]SEConfig, error) {
	q := fieldsExtra("uuid,name,cloud_ref,tenant_ref,se_group_ref,controller_ip,availability_zone,mgmt_vnic,data_vnics")
	q.Set("include_name", "true")
	return listAll[SEConfig](ctx, c, "/api/serviceengine", "admin", q)
}

// ListVsVipInventory returns all VsVip inventory entries for the given tenant.
func (c *Client) ListVsVipInventory(ctx context.Context, tenant string) ([]VsVipInventoryItem, error) {
	return listAll[VsVipInventoryItem](ctx, c, "/api/vsvip-inventory", tenant, inventoryExtra())
}

// ListPoolGroupInventory returns pool-group inventory entries for the given tenant.
func (c *Client) ListPoolGroupInventory(ctx context.Context, tenant string) ([]PoolGroupInventoryItem, error) {
	q := url.Values{}
	q.Set("include_name", "true")
	return listAll[PoolGroupInventoryItem](ctx, c, "/api/poolgroup-inventory", tenant, q)
}

// ListGslbServiceInventory returns GSLB service inventory entries for the
// given tenant. GSLB services are admin/federated objects on most setups;
// callers usually pass tenant="admin" or "*".
func (c *Client) ListGslbServiceInventory(ctx context.Context, tenant string) ([]GslbServiceInventoryItem, error) {
	return listAll[GslbServiceInventoryItem](ctx, c, "/api/gslbservice-inventory", tenant, inventoryExtra())
}

// GetPoolRuntimeDetail returns the per-server runtime for one pool. The
// canonical 22.1+ endpoint is /api/pool/{uuid}/runtime/server/detail/. Avi
// versions differ: some return the normal page envelope {results,next}, some
// return an array directly, and others wrap it in {server:[...]}.
func (c *Client) GetPoolRuntimeDetail(ctx context.Context, tenant, poolUUID string) ([]ServerRuntimeDetail, error) {
	path := "/api/pool/" + poolUUID + "/runtime/server/detail/"
	var raw json.RawMessage
	if err := c.Get(ctx, path, &raw, RequestOptions{Tenant: tenant, Query: pageQuery(1)}); err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	// Try bare array first (most common 22.1+ shape).
	var arr []ServerRuntimeDetail
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr, nil
	}

	var page PageResp[ServerRuntimeDetail]
	if err := json.Unmarshal(raw, &page); err == nil && (page.Results != nil || page.Next != nil || page.Count > 0) {
		return c.listPoolRuntimeDetailPages(ctx, path, tenant, page)
	}

	// Fall back to wrapped {server:[...]}.
	var wrapped PoolRuntimeDetail
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("unmarshal pool runtime detail (%s): %w", path, err)
	}
	return wrapped.Server, nil
}

func pageQuery(page int) url.Values {
	q := url.Values{}
	q.Set("page", strconv.Itoa(page))
	q.Set("page_size", strconv.Itoa(defaultPageSize))
	return q
}

func (c *Client) listPoolRuntimeDetailPages(ctx context.Context, path, tenant string, first PageResp[ServerRuntimeDetail]) ([]ServerRuntimeDetail, error) {
	all := append([]ServerRuntimeDetail{}, first.Results...)
	if poolRuntimeDetailDone(first, len(all)) {
		return all, nil
	}

	for page := 2; page <= maxPoolRuntimeDetailPages; page++ {
		var resp PageResp[ServerRuntimeDetail]
		if err := c.Get(ctx, path, &resp, RequestOptions{Tenant: tenant, Query: pageQuery(page)}); err != nil {
			return nil, fmt.Errorf("list %s page %d: %w", path, page, err)
		}
		all = append(all, resp.Results...)
		if poolRuntimeDetailDone(resp, len(all)) {
			return all, nil
		}
	}
	return nil, fmt.Errorf("list %s exceeded %d pages", path, maxPoolRuntimeDetailPages)
}

func poolRuntimeDetailDone(resp PageResp[ServerRuntimeDetail], total int) bool {
	if resp.Count > 0 && total >= resp.Count {
		return true
	}
	if len(resp.Results) < defaultPageSize {
		return true
	}
	return resp.Next == nil || *resp.Next == ""
}
