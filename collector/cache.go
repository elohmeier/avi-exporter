package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

const (
	defaultRefreshInterval = 2 * time.Minute
	defaultRefreshTimeout  = 120 * time.Second
)

type moduleKey struct {
	Module string
	Tenant string
}

type moduleState struct {
	Module       string        `json:"module"`
	Tenant       string        `json:"tenant"`
	LastAttempt  time.Time     `json:"last_attempt,omitempty"`
	LastSuccess  time.Time     `json:"last_success,omitempty"`
	LastDuration time.Duration `json:"-"`
	Attempts     uint64        `json:"attempts"`
	Errors       uint64        `json:"errors"`
	LastError    string        `json:"last_error,omitempty"`
	MaxStale     time.Duration `json:"-"`
	Required     bool          `json:"required"`
}

type modulePolicy struct {
	timeout  time.Duration
	maxStale time.Duration
	required bool
}

type cacheModuleStatus struct {
	Module              string  `json:"module"`
	Tenant              string  `json:"tenant"`
	LastAttemptUnix     int64   `json:"last_attempt_unix,omitempty"`
	LastSuccessUnix     int64   `json:"last_success_unix,omitempty"`
	LastDurationSeconds float64 `json:"last_duration_seconds"`
	AgeSeconds          float64 `json:"age_seconds"`
	MaxStaleSeconds     float64 `json:"max_stale_seconds"`
	Stale               bool    `json:"stale"`
	Required            bool    `json:"required"`
	Attempts            uint64  `json:"attempts"`
	Errors              uint64  `json:"errors"`
	LastError           string  `json:"last_error,omitempty"`
}

type cacheStatus struct {
	Ready   bool                `json:"ready"`
	Tenants []string            `json:"tenants"`
	Modules []cacheModuleStatus `json:"modules"`
}

// Start launches the background refresh scheduler. The first refresh is
// attempted immediately, then repeated at the configured cache cadence.
func (e *Exporter) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		e.schedulerCancel = cancel
		go e.schedulerLoop(runCtx)
	})
}

// Stop cancels a scheduler started with Start.
func (e *Exporter) Stop() {
	if e.schedulerCancel != nil {
		e.schedulerCancel()
	}
}

func (e *Exporter) schedulerLoop(ctx context.Context) {
	e.runRefreshWithLog(ctx)

	ticker := time.NewTicker(e.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runRefreshWithLog(ctx)
		}
	}
}

func (e *Exporter) runRefreshWithLog(ctx context.Context) {
	if err := e.RefreshOnce(ctx); err != nil && e.logger != nil {
		e.logger.Warn("background refresh completed with errors", "err", err)
	}
}

// RefreshOnce refreshes every enabled module once. It is safe to call from
// tests and from the scheduler; calls are serialized so module state remains
// coherent and Avi API fanout is bounded.
func (e *Exporter) RefreshOnce(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.refreshMu.Lock()
	defer e.refreshMu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, e.refreshTimeout)
	defer cancel()

	var errs []error
	tenants, err := e.refreshTenants(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	if !e.cfg.IsModuleDisabled("cluster") {
		if err := e.runModule(ctx, "cluster", "", func(ctx context.Context) error {
			return e.refreshCluster(ctx)
		}); err != nil {
			errs = append(errs, err)
		}
	}

	if !e.cfg.IsModuleDisabled("controller_metrics") {
		if err := e.runModule(ctx, "controller_metrics", "", func(ctx context.Context) error {
			return e.collectControllerAnalytics(ctx)
		}); err != nil {
			errs = append(errs, err)
		}
	}

	if err := e.refreshServiceEngines(ctx); err != nil {
		errs = append(errs, err)
	}

	if err := e.refreshTenantSet(ctx, tenants); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (e *Exporter) refreshTenants(ctx context.Context) ([]string, error) {
	static, wildcard := e.configuredTenants()
	if !wildcard {
		e.cacheMu.Lock()
		e.setTenantsLocked(static)
		e.cacheMu.Unlock()
		return static, nil
	}

	var discovered []string
	err := e.runModule(ctx, "tenant_discovery", "", func(ctx context.Context) error {
		tenants, err := e.client.LoginTenants(ctx)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(static)+len(tenants))
		names = append(names, static...)
		for _, t := range tenants {
			names = append(names, t.Name)
		}
		if len(tenants) == 0 {
			tenants, err = e.client.ListTenants(ctx)
			if err != nil {
				return err
			}
			for _, t := range tenants {
				names = append(names, t.Name)
			}
		}
		discovered = normalizeTenantNames(names)

		e.cacheMu.Lock()
		e.setTenantsLocked(discovered)
		e.cacheMu.Unlock()
		return nil
	})
	if err == nil {
		return discovered, nil
	}

	e.cacheMu.Lock()
	cached := append([]string{}, e.tenants...)
	e.cacheMu.Unlock()
	if len(cached) > 0 {
		return cached, err
	}
	if len(static) > 0 {
		e.cacheMu.Lock()
		e.setTenantsLocked(static)
		e.cacheMu.Unlock()
		return static, err
	}
	return nil, err
}

func (e *Exporter) configuredTenants() ([]string, bool) {
	var names []string
	wildcard := false
	for _, t := range e.cfg.Tenants {
		if t == "*" {
			wildcard = true
			continue
		}
		names = append(names, t)
	}
	if len(names) == 0 && !wildcard {
		names = []string{"admin"}
	}
	if len(names) == 0 {
		return nil, wildcard
	}
	return normalizeTenants(names), wildcard
}

func (e *Exporter) setTenantsLocked(next []string) {
	current := make(map[string]bool, len(e.tenants))
	for _, tenant := range e.tenants {
		current[tenant] = true
	}
	for _, tenant := range next {
		delete(current, tenant)
	}
	for tenant := range current {
		e.removeTenantCacheLocked(tenant)
	}
	e.tenants = append([]string{}, next...)
}

func (e *Exporter) removeTenantCacheLocked(tenant string) {
	gauges := append([]*prometheus.GaugeVec{}, e.allGaugeVecs()...)
	gauges = append(gauges,
		e.up,
		e.scrapeDuration,
		e.moduleLastSuccess,
		e.moduleLastAttempt,
		e.moduleAge,
		e.moduleStale,
		e.moduleRefreshDuration,
	)
	deleteTenantGaugeVecs(tenant, gauges...)
	deleteTenantCounterVecs(tenant,
		e.scrapeErrorsTotal,
		e.scrapeTotal,
		e.moduleRefreshErrorsTotal,
		e.moduleRefreshTotal,
	)

	for key := range e.moduleStates {
		if key.Tenant == tenant {
			delete(e.moduleStates, key)
		}
	}
}

func normalizeTenants(in []string) []string {
	out := normalizeTenantNames(in)
	if len(out) == 0 {
		return []string{"admin"}
	}
	return out
}

func normalizeTenantNames(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, n := range in {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func (e *Exporter) refreshCluster(ctx context.Context) error {
	rt, err := e.client.GetClusterRuntime(ctx)
	if err != nil {
		return err
	}

	clusterUp := 0.0
	if rt.ClusterState.State == "CLUSTER_UP_HA_ACTIVE" || rt.ClusterState.State == "CLUSTER_UP_NO_HA" {
		clusterUp = 1
	}

	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	resetGaugeVecs(e.clusterStateInfo, e.clusterNodeUp, e.clusterNodeRole)
	e.clusterUpValue = clusterUp
	e.clusterProgressValue = float64(rt.ClusterState.Progress)
	e.clusterCached = true
	e.emitInfo(e.clusterStateInfo, e.buildBaseLabels(), "state", rt.ClusterState.State)

	for _, n := range rt.NodeStates {
		labels := e.appendLabels(n.Name)
		up := 0.0
		if n.State == "CLUSTER_ACTIVE" {
			up = 1
		}
		e.clusterNodeUp.WithLabelValues(labels...).Set(up)

		leader := 0.0
		if n.Role == "CLUSTER_LEADER" {
			leader = 1
		}
		e.clusterNodeRole.WithLabelValues(labels...).Set(leader)
	}
	return nil
}

func (e *Exporter) refreshServiceEngines(ctx context.Context) error {
	if e.cfg.IsModuleDisabled("se_inventory") && e.cfg.IsModuleDisabled("se_metrics") {
		return nil
	}

	var errs []error
	var seItems []avi.SEInventoryItem
	if !e.cfg.IsModuleDisabled("se_inventory") {
		if err := e.runModule(ctx, "se_list", "", func(ctx context.Context) error {
			items, err := e.client.ListSEInventory(ctx)
			if err != nil {
				return err
			}
			seItems = items
			return nil
		}); err != nil {
			errs = append(errs, err)
		} else if err := e.runModule(ctx, "se_inventory", "", func(ctx context.Context) error {
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			resetGaugeVecs(e.seInventoryGaugeVecs()...)
			e.collectSEInventory(ctx, seItems, nil)
			return nil
		}); err != nil {
			errs = append(errs, err)
		}
	}

	if !e.cfg.IsModuleDisabled("se_metrics") {
		if err := e.runModule(ctx, "se_metrics", "", func(ctx context.Context) error {
			return e.collectSEAnalytics(ctx, seItems, nil)
		}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Exporter) refreshTenantSet(ctx context.Context, tenants []string) error {
	if len(tenants) == 0 {
		return nil
	}

	sem := make(chan struct{}, e.parallelism)
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	record := func(err error) {
		if err == nil {
			return
		}
		errsMu.Lock()
		errs = append(errs, err)
		errsMu.Unlock()
	}

	for _, tenant := range tenants {
		tenant := tenant
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				record(ctx.Err())
				return
			default:
			}
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				record(ctx.Err())
				return
			}
			record(e.refreshTenant(ctx, tenant))
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (e *Exporter) refreshTenant(ctx context.Context, tenant string) error {
	var errs []error
	tenantOK := true
	record := func(err error) {
		if err != nil {
			tenantOK = false
			errs = append(errs, err)
		}
	}

	var vsItems []avi.VSInventoryItem
	vsListOK := false
	needVS := !e.cfg.IsModuleDisabled("vs_inventory") || !e.cfg.IsModuleDisabled("vs_metrics") || !e.cfg.IsModuleDisabled("topology")
	if needVS {
		record(e.runModule(ctx, "vs_list", tenant, func(ctx context.Context) error {
			items, err := e.client.ListVSInventory(ctx, tenant)
			if err != nil {
				return err
			}
			vsItems = items
			vsListOK = true
			return nil
		}))
	}
	if !e.cfg.IsModuleDisabled("vs_inventory") && vsListOK {
		record(e.runModule(ctx, "vs_inventory", tenant, func(ctx context.Context) error {
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.vsInventoryGaugeVecs()...)
			e.collectVSInventory(ctx, tenant, vsItems, nil)
			return nil
		}))
	}
	if !e.cfg.IsModuleDisabled("vs_metrics") && vsListOK {
		record(e.runModule(ctx, "vs_metrics", tenant, func(ctx context.Context) error {
			return e.collectVSAnalytics(ctx, tenant, vsItems, nil)
		}))
	}

	var poolItems []avi.PoolInventoryItem
	poolListOK := false
	needPools := !e.cfg.IsModuleDisabled("pool_inventory") || !e.cfg.IsModuleDisabled("pool_metrics") ||
		!e.cfg.IsModuleDisabled("pool_members") || !e.cfg.IsModuleDisabled("topology")
	if needPools {
		record(e.runModule(ctx, "pool_list", tenant, func(ctx context.Context) error {
			items, err := e.client.ListPoolInventory(ctx, tenant)
			if err != nil {
				return err
			}
			poolItems = items
			poolListOK = true
			return nil
		}))
	}
	if !e.cfg.IsModuleDisabled("pool_inventory") && poolListOK {
		record(e.runModule(ctx, "pool_inventory", tenant, func(ctx context.Context) error {
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.poolInventoryGaugeVecs()...)
			e.collectPoolInventory(ctx, tenant, poolItems, nil)
			return nil
		}))
	}
	if !e.cfg.IsModuleDisabled("pool_metrics") && poolListOK {
		record(e.runModule(ctx, "pool_metrics", tenant, func(ctx context.Context) error {
			return e.collectPoolAnalytics(ctx, tenant, poolItems, nil)
		}))
	}

	var poolMembers []poolMemberSnapshot
	poolMembersOK := e.cfg.IsModuleDisabled("pool_members")
	if !e.cfg.IsModuleDisabled("pool_members") && poolListOK {
		record(e.runModule(ctx, "pool_members", tenant, func(ctx context.Context) error {
			members, err := e.collectPoolMemberDetails(ctx, tenant, poolItems)
			if err != nil {
				return err
			}
			poolMembers = members
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.poolMemberGaugeVecs()...)
			e.renderPoolMembers(tenant, members)
			poolMembersOK = true
			return nil
		}))
	}

	var vsvipItems []avi.VsVipInventoryItem
	vsvipListOK := false
	needVsVip := !e.cfg.IsModuleDisabled("vsvip") || !e.cfg.IsModuleDisabled("topology")
	if needVsVip {
		record(e.runModule(ctx, "vsvip_list", tenant, func(ctx context.Context) error {
			items, err := e.client.ListVsVipInventory(ctx, tenant)
			if err != nil {
				return err
			}
			vsvipItems = items
			vsvipListOK = true
			return nil
		}))
	}
	if !e.cfg.IsModuleDisabled("vsvip") && vsvipListOK {
		record(e.runModule(ctx, "vsvip", tenant, func(ctx context.Context) error {
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.vsvipGaugeVecs()...)
			e.collectVsVipInventory(ctx, tenant, vsvipItems, nil)
			return nil
		}))
	}

	var poolGroupItems []avi.PoolGroupInventoryItem
	poolGroupOK := e.cfg.IsModuleDisabled("pool_group")
	if !e.cfg.IsModuleDisabled("pool_group") {
		record(e.runModule(ctx, "pool_group", tenant, func(ctx context.Context) error {
			items, err := e.client.ListPoolGroupInventory(ctx, tenant)
			if err != nil {
				return err
			}
			poolGroupItems = items
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.poolGroupGaugeVecs()...)
			e.collectPoolGroupInventory(ctx, tenant, items, nil)
			poolGroupOK = true
			return nil
		}))
	}

	if !e.cfg.IsModuleDisabled("gslb") {
		record(e.runModule(ctx, "gslb", tenant, func(ctx context.Context) error {
			items, err := e.client.ListGslbServiceInventory(ctx, tenant)
			if err != nil {
				return err
			}
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.gslbGaugeVecs()...)
			e.collectGslbServices(ctx, tenant, items, nil)
			return nil
		}))
	}

	if !e.cfg.IsModuleDisabled("topology") {
		record(e.runModule(ctx, "topology", tenant, func(ctx context.Context) error {
			if !vsListOK || !poolListOK || !vsvipListOK {
				return fmt.Errorf("topology requires fresh VS, pool, and VsVip inventory")
			}
			if !poolGroupOK {
				return fmt.Errorf("topology requires fresh pool group data")
			}
			if !poolMembersOK {
				return fmt.Errorf("topology requires fresh pool member data")
			}
			e.cacheMu.Lock()
			defer e.cacheMu.Unlock()
			deleteTenantGaugeVecs(tenant, e.topologyGaugeVecs()...)
			e.collectTopology(tenant, vsItems, poolItems, vsvipItems, nil)
			e.renderPoolGroupTopology(tenant, poolGroupItems)
			e.renderPoolMemberTopology(tenant, poolMembers)
			return nil
		}))
	}

	e.cacheMu.Lock()
	val := 1.0
	if !tenantOK {
		val = 0
	}
	e.up.WithLabelValues(e.appendLabels(tenant)...).Set(val)
	e.cacheMu.Unlock()

	return errors.Join(errs...)
}

func (e *Exporter) runModule(ctx context.Context, module, tenant string, fn func(context.Context) error) error {
	policy := policyForModule(module)
	moduleCtx, cancel := context.WithTimeout(ctx, policy.timeout)
	defer cancel()

	start := time.Now()
	err := fn(moduleCtx)
	finished := time.Now()
	duration := finished.Sub(start)

	labels := e.appendLabels(module, tenant)
	e.cacheMu.Lock()
	st := e.ensureModuleStateLocked(module, tenant)
	st.LastAttempt = finished
	st.LastDuration = duration
	st.Attempts++
	st.MaxStale = policy.maxStale
	st.Required = policy.required
	if err != nil {
		st.Errors++
		st.LastError = err.Error()
	} else {
		st.LastSuccess = finished
		st.LastError = ""
	}

	e.scrapeDuration.WithLabelValues(labels...).Set(duration.Seconds())
	e.moduleRefreshDuration.WithLabelValues(labels...).Set(duration.Seconds())
	e.scrapeTotal.WithLabelValues(labels...).Inc()
	e.moduleRefreshTotal.WithLabelValues(labels...).Inc()
	e.moduleLastAttempt.WithLabelValues(labels...).Set(float64(finished.Unix()))
	if st.LastSuccess.IsZero() {
		e.moduleLastSuccess.WithLabelValues(labels...).Set(0)
	} else {
		e.moduleLastSuccess.WithLabelValues(labels...).Set(float64(st.LastSuccess.Unix()))
	}
	if err != nil {
		e.scrapeErrorsTotal.WithLabelValues(labels...).Inc()
		e.moduleRefreshErrorsTotal.WithLabelValues(labels...).Inc()
	}
	e.cacheMu.Unlock()

	if err != nil && e.logger != nil {
		e.logger.Error("module refresh failed", "module", module, "tenant", tenant, "err", err)
	}
	return err
}

func (e *Exporter) ensureModuleStateLocked(module, tenant string) *moduleState {
	key := moduleKey{Module: module, Tenant: tenant}
	if st := e.moduleStates[key]; st != nil {
		return st
	}
	policy := policyForModule(module)
	st := &moduleState{
		Module:   module,
		Tenant:   tenant,
		MaxStale: policy.maxStale,
		Required: policy.required,
	}
	e.moduleStates[key] = st
	return st
}

func policyForModule(module string) modulePolicy {
	policy := modulePolicy{
		timeout:  60 * time.Second,
		maxStale: 10 * time.Minute,
		required: true,
	}
	switch module {
	case "tenant_discovery":
		policy.timeout = 60 * time.Second
		policy.maxStale = 30 * time.Minute
	case "vs_metrics", "pool_metrics", "se_metrics", "controller_metrics":
		policy.timeout = 90 * time.Second
		policy.maxStale = 15 * time.Minute
	case "pool_members":
		policy.timeout = 90 * time.Second
		policy.maxStale = 20 * time.Minute
	case "topology":
		policy.timeout = 30 * time.Second
		policy.maxStale = 10 * time.Minute
	}
	return policy
}

func (e *Exporter) expectedTenantsLocked() []string {
	if len(e.tenants) > 0 {
		return append([]string{}, e.tenants...)
	}
	static, wildcard := e.configuredTenants()
	if wildcard {
		return nil
	}
	return static
}

func (e *Exporter) requiredModuleKeysLocked() []moduleKey {
	keys := make([]moduleKey, 0)
	add := func(module, tenant string) {
		if policyForModule(module).required {
			keys = append(keys, moduleKey{Module: module, Tenant: tenant})
		}
	}

	_, wildcard := e.configuredTenants()
	if wildcard {
		add("tenant_discovery", "")
	}
	if !e.cfg.IsModuleDisabled("cluster") {
		add("cluster", "")
	}
	if !e.cfg.IsModuleDisabled("controller_metrics") {
		add("controller_metrics", "")
	}
	if !e.cfg.IsModuleDisabled("se_inventory") {
		add("se_list", "")
	}
	if !e.cfg.IsModuleDisabled("se_inventory") {
		add("se_inventory", "")
	}
	if !e.cfg.IsModuleDisabled("se_metrics") {
		add("se_metrics", "")
	}

	for _, tenant := range e.expectedTenantsLocked() {
		needVS := !e.cfg.IsModuleDisabled("vs_inventory") || !e.cfg.IsModuleDisabled("vs_metrics") || !e.cfg.IsModuleDisabled("topology")
		if needVS {
			add("vs_list", tenant)
		}
		if !e.cfg.IsModuleDisabled("vs_inventory") {
			add("vs_inventory", tenant)
		}
		if !e.cfg.IsModuleDisabled("vs_metrics") {
			add("vs_metrics", tenant)
		}

		needPools := !e.cfg.IsModuleDisabled("pool_inventory") || !e.cfg.IsModuleDisabled("pool_metrics") ||
			!e.cfg.IsModuleDisabled("pool_members") || !e.cfg.IsModuleDisabled("topology")
		if needPools {
			add("pool_list", tenant)
		}
		if !e.cfg.IsModuleDisabled("pool_inventory") {
			add("pool_inventory", tenant)
		}
		if !e.cfg.IsModuleDisabled("pool_metrics") {
			add("pool_metrics", tenant)
		}
		if !e.cfg.IsModuleDisabled("pool_members") {
			add("pool_members", tenant)
		}

		needVsVip := !e.cfg.IsModuleDisabled("vsvip") || !e.cfg.IsModuleDisabled("topology")
		if needVsVip {
			add("vsvip_list", tenant)
		}
		if !e.cfg.IsModuleDisabled("vsvip") {
			add("vsvip", tenant)
		}

		if !e.cfg.IsModuleDisabled("pool_group") {
			add("pool_group", tenant)
		}
		if !e.cfg.IsModuleDisabled("gslb") {
			add("gslb", tenant)
		}
		if !e.cfg.IsModuleDisabled("topology") {
			add("topology", tenant)
		}
	}

	return keys
}

func (e *Exporter) updateDynamicSelfMetricsLocked(now time.Time) {
	for _, st := range e.moduleStates {
		labels := e.appendLabels(st.Module, st.Tenant)
		age := 0.0
		stale := 1.0
		if !st.LastSuccess.IsZero() {
			age = now.Sub(st.LastSuccess).Seconds()
			if !st.isStale(now) {
				stale = 0
			}
		}
		e.moduleAge.WithLabelValues(labels...).Set(age)
		e.moduleStale.WithLabelValues(labels...).Set(stale)
	}
}

func (st *moduleState) isStale(now time.Time) bool {
	if st.LastSuccess.IsZero() {
		return true
	}
	maxStale := st.MaxStale
	if maxStale <= 0 {
		maxStale = 10 * time.Minute
	}
	return now.Sub(st.LastSuccess) > maxStale
}

// Ready returns true when all required modules have known fresh state.
func (e *Exporter) Ready() bool {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()

	expected := e.requiredModuleKeysLocked()
	if len(expected) == 0 {
		return false
	}
	now := time.Now()
	for _, key := range expected {
		st := e.moduleStates[key]
		if st == nil || st.isStale(now) {
			return false
		}
	}
	for _, st := range e.moduleStates {
		if st.Required && st.isStale(now) {
			return false
		}
	}
	return true
}

func (e *Exporter) CacheStatus() cacheStatus {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()

	now := time.Now()
	expected := e.requiredModuleKeysLocked()
	status := cacheStatus{
		Ready:   len(expected) > 0,
		Tenants: append([]string{}, e.tenants...),
		Modules: make([]cacheModuleStatus, 0, len(e.moduleStates)),
	}
	for _, key := range expected {
		st := e.moduleStates[key]
		if st == nil || st.isStale(now) {
			status.Ready = false
			break
		}
	}
	for _, st := range e.moduleStates {
		stale := st.isStale(now)
		if st.Required && stale {
			status.Ready = false
		}

		age := 0.0
		if !st.LastSuccess.IsZero() {
			age = now.Sub(st.LastSuccess).Seconds()
		}
		module := cacheModuleStatus{
			Module:              st.Module,
			Tenant:              st.Tenant,
			LastDurationSeconds: st.LastDuration.Seconds(),
			AgeSeconds:          age,
			MaxStaleSeconds:     st.MaxStale.Seconds(),
			Stale:               stale,
			Required:            st.Required,
			Attempts:            st.Attempts,
			Errors:              st.Errors,
			LastError:           st.LastError,
		}
		if !st.LastAttempt.IsZero() {
			module.LastAttemptUnix = st.LastAttempt.Unix()
		}
		if !st.LastSuccess.IsZero() {
			module.LastSuccessUnix = st.LastSuccess.Unix()
		}
		status.Modules = append(status.Modules, module)
	}
	sort.Slice(status.Modules, func(i, j int) bool {
		if status.Modules[i].Module == status.Modules[j].Module {
			return status.Modules[i].Tenant < status.Modules[j].Tenant
		}
		return status.Modules[i].Module < status.Modules[j].Module
	})
	return status
}

func (e *Exporter) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if e.Ready() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
		return
	}
	http.Error(w, "cache is not ready", http.StatusServiceUnavailable)
}

func (e *Exporter) DebugCacheHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(e.CacheStatus())
}

func (e *Exporter) vsInventoryGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.vsOperUp, e.vsOperStatusInfo, e.vsEnabled, e.vsHealthScore, e.vsPercentSesUp, e.vsTypeInfo, e.vsAlertLevel,
		e.vsVipOperUp, e.vsVipPercentSesUp, e.vsVipNumSeAssigned, e.vsVipNumSeRequested, e.vsVipOperStatusInfo,
	}
}

func (e *Exporter) controllerMetricsGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.controllerAvgCPUUsage, e.controllerAvgMemUsage, e.controllerAvgDiskUsage,
		e.controllerAvgDiskReadBytes, e.controllerAvgDiskWriteBytes,
		e.controllerAvgNumActiveVS, e.controllerAvgNumBackendServers,
	}
}

func (e *Exporter) vsAnalyticsGaugeVecs() []*prometheus.GaugeVec {
	return sortedUniqueGaugeVecs(e.vsAnalyticsGauges)
}

func (e *Exporter) poolInventoryGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.poolOperUp, e.poolOperStatusInfo, e.poolEnabled, e.poolHealthScore,
		e.poolNumServers, e.poolNumServersUp, e.poolNumServersEnabled,
		e.poolPercentServersUpEnabled, e.poolPercentServersUpTotal,
		e.poolAlertLevel, e.poolAppProfileType,
	}
}

func (e *Exporter) poolMemberGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{e.poolMemberOperUp, e.poolMemberOperStatusInfo}
}

func (e *Exporter) poolAnalyticsGaugeVecs() []*prometheus.GaugeVec {
	return sortedUniqueGaugeVecs(e.poolAnalyticsGauges)
}

func (e *Exporter) poolGroupGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{e.poolGroupInfo, e.poolGroupMemberCount}
}

func (e *Exporter) gslbGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.gslbServiceOperUp, e.gslbServiceOperStatusInfo, e.gslbServiceEnabled,
		e.gslbServiceMemberCount, e.gslbServiceDomainsInfo,
	}
}

func (e *Exporter) seInventoryGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.seOperUp, e.seOperStatusInfo, e.seEnabled, e.seHealthScore,
		e.seConnected, e.seBgpPeersUp, e.seGatewayUp, e.seAtCurrVer, e.seSufficientMem, e.seLicensedCores,
		e.seLicenseState, e.sePowerState, e.seMigrateState, e.seVersionInfo, e.seEnableStateInfo,
	}
}

func (e *Exporter) seAnalyticsGaugeVecs() []*prometheus.GaugeVec {
	return sortedUniqueGaugeVecs(e.seAnalyticsGauges)
}

func (e *Exporter) vsvipGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.vipOperUp, e.vipOperStatusInfo, e.vipEnabled, e.vipPercentSesUp, e.vipNumSeAssigned, e.vipNumSeRequested,
		e.vipActiveOnSe, e.vipSharedByVsCount, e.vipFloatingIP, e.vipAutoAllocated, e.vipDNSRecord,
	}
}

func (e *Exporter) topologyGaugeVecs() []*prometheus.GaugeVec {
	return []*prometheus.GaugeVec{
		e.topologyNode, e.topologyEdge, e.topologyNodeState, e.topologyNodeHealth,
		e.topologyNodeRequestsTotal, e.topologyNodeConnections,
	}
}
