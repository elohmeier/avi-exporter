package collector

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/avi-exporter/avi"
)

// Collect is invoked by the Prometheus HTTP handler on each scrape.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Reset all GaugeVecs at the top so deleted entities don't linger as
	// stale series. (CounterVecs are immune by design.)
	e.resetGauges()

	tenants := e.resolveTenants(ctx)

	// 1. Cluster — admin tenant only, no per-tenant fanout.
	if !e.cfg.IsModuleDisabled("cluster") {
		e.runModule(ch, "cluster", "", func() error {
			return e.collectCluster(ctx, ch)
		})
	}

	// 2. Service Engines — admin tenant.
	var seItems []avi.SEInventoryItem
	if !e.cfg.IsModuleDisabled("se_inventory") || !e.cfg.IsModuleDisabled("se_metrics") {
		e.runModule(ch, "se_list", "", func() error {
			items, err := e.client.ListSEInventory(ctx)
			if err != nil {
				return err
			}
			seItems = items
			return nil
		})
	}
	if !e.cfg.IsModuleDisabled("se_inventory") && seItems != nil {
		e.runModule(ch, "se_inventory", "", func() error {
			e.collectSEInventory(ctx, seItems, ch)
			return nil
		})
	}
	if !e.cfg.IsModuleDisabled("se_metrics") && seItems != nil {
		e.runModule(ch, "se_metrics", "", func() error {
			return e.collectSEAnalytics(ctx, seItems, ch)
		})
	}

	// 3. Per-tenant fanout.
	sem := make(chan struct{}, e.parallelism)
	var wg sync.WaitGroup
	for _, tenant := range tenants {
		tenant := tenant
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			e.scrapeTenant(ctx, tenant, ch)
		}()
	}
	wg.Wait()

	// 4. Emit topology after all tenants have populated their nodes/edges.
	if !e.cfg.IsModuleDisabled("topology") {
		e.emitTopology(ch)
	}

	// 5. Self-metrics
	e.up.Collect(ch)
	e.scrapeDuration.Collect(ch)
	e.scrapeErrorsTotal.Collect(ch)
	e.scrapeTotal.Collect(ch)
}

// scrapeTenant runs the per-tenant modules sequentially against a shared client.
func (e *Exporter) scrapeTenant(ctx context.Context, tenant string, ch chan<- prometheus.Metric) {
	tenantOK := true

	var vsItems []avi.VSInventoryItem
	if !e.cfg.IsModuleDisabled("vs_inventory") || !e.cfg.IsModuleDisabled("vs_metrics") || !e.cfg.IsModuleDisabled("topology") {
		if err := e.runModule(ch, "vs_list", tenant, func() error {
			items, err := e.client.ListVSInventory(ctx, tenant)
			if err != nil {
				return err
			}
			vsItems = items
			return nil
		}); err != nil {
			tenantOK = false
		}
	}
	if !e.cfg.IsModuleDisabled("vs_inventory") && vsItems != nil {
		e.runModule(ch, "vs_inventory", tenant, func() error {
			e.collectVSInventory(ctx, tenant, vsItems, ch)
			return nil
		})
	}
	if !e.cfg.IsModuleDisabled("vs_metrics") && vsItems != nil {
		if err := e.runModule(ch, "vs_metrics", tenant, func() error {
			return e.collectVSAnalytics(ctx, tenant, vsItems, ch)
		}); err != nil {
			tenantOK = false
		}
	}

	var poolItems []avi.PoolInventoryItem
	if !e.cfg.IsModuleDisabled("pool_inventory") || !e.cfg.IsModuleDisabled("pool_metrics") || !e.cfg.IsModuleDisabled("pool_members") || !e.cfg.IsModuleDisabled("topology") {
		if err := e.runModule(ch, "pool_list", tenant, func() error {
			items, err := e.client.ListPoolInventory(ctx, tenant)
			if err != nil {
				return err
			}
			poolItems = items
			return nil
		}); err != nil {
			tenantOK = false
		}
	}
	if !e.cfg.IsModuleDisabled("pool_inventory") && poolItems != nil {
		e.runModule(ch, "pool_inventory", tenant, func() error {
			e.collectPoolInventory(ctx, tenant, poolItems, ch)
			return nil
		})
	}
	if !e.cfg.IsModuleDisabled("pool_metrics") && poolItems != nil {
		if err := e.runModule(ch, "pool_metrics", tenant, func() error {
			return e.collectPoolAnalytics(ctx, tenant, poolItems, ch)
		}); err != nil {
			tenantOK = false
		}
	}
	if !e.cfg.IsModuleDisabled("pool_members") && poolItems != nil {
		if err := e.runModule(ch, "pool_members", tenant, func() error {
			return e.collectPoolMembers(ctx, tenant, poolItems, ch)
		}); err != nil {
			tenantOK = false
		}
	}

	var vsvipItems []avi.VsVipInventoryItem
	if !e.cfg.IsModuleDisabled("vsvip") || !e.cfg.IsModuleDisabled("topology") {
		if err := e.runModule(ch, "vsvip_list", tenant, func() error {
			items, err := e.client.ListVsVipInventory(ctx, tenant)
			if err != nil {
				return err
			}
			vsvipItems = items
			return nil
		}); err != nil {
			tenantOK = false
		}
	}
	if !e.cfg.IsModuleDisabled("vsvip") && vsvipItems != nil {
		e.runModule(ch, "vsvip", tenant, func() error {
			e.collectVsVipInventory(ctx, tenant, vsvipItems, ch)
			return nil
		})
	}

	if !e.cfg.IsModuleDisabled("pool_group") {
		e.runModule(ch, "pool_group", tenant, func() error {
			items, err := e.client.ListPoolGroupInventory(ctx, tenant)
			if err != nil {
				return err
			}
			e.collectPoolGroupInventory(ctx, tenant, items, ch)
			return nil
		})
	}

	if !e.cfg.IsModuleDisabled("gslb") {
		e.runModule(ch, "gslb", tenant, func() error {
			items, err := e.client.ListGslbServiceInventory(ctx, tenant)
			if err != nil {
				return err
			}
			e.collectGslbServices(ctx, tenant, items, ch)
			return nil
		})
	}

	if !e.cfg.IsModuleDisabled("topology") {
		e.collectTopology(tenant, vsItems, poolItems, vsvipItems, ch)
	}

	// Per-tenant up gauge — 1 iff *every* attempted module call succeeded.
	val := 1.0
	if !tenantOK {
		val = 0
	}
	e.up.WithLabelValues(e.appendLabels(tenant)...).Set(val)
}

// runModule wraps one collector call with timing, error counting, and
// scrape_total/errors_total bookkeeping. tenant may be empty for global modules.
func (e *Exporter) runModule(ch chan<- prometheus.Metric, module, tenant string, fn func() error) error {
	start := time.Now()
	err := fn()
	dur := time.Since(start).Seconds()

	labels := e.appendLabels(module, tenant)
	e.scrapeDuration.WithLabelValues(labels...).Set(dur)
	e.scrapeTotal.WithLabelValues(labels...).Inc()
	if err != nil {
		e.logger.Error("module scrape failed", "module", module, "tenant", tenant, "err", err)
		e.scrapeErrorsTotal.WithLabelValues(labels...).Inc()
	}
	return err
}

// resolveTenants returns the effective tenant list. "*" is expanded via
// /api/tenant; on failure we fall back to {"admin"} so something still works.
// Output is sorted so cardinality doesn't jitter between scrapes.
func (e *Exporter) resolveTenants(ctx context.Context) []string {
	var names []string
	for _, t := range e.cfg.Tenants {
		if t != "*" {
			names = append(names, t)
			continue
		}
		ts, err := e.client.ListTenants(ctx)
		if err != nil {
			e.logger.Error("list tenants for wildcard expansion", "err", err)
			return []string{"admin"}
		}
		for _, t := range ts {
			names = append(names, t.Name)
		}
	}
	if len(names) == 0 {
		return []string{"admin"}
	}
	// Dedup + sort.
	seen := make(map[string]bool, len(names))
	out := names[:0]
	for _, n := range names {
		if seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// resetGauges clears all GaugeVecs at the start of a scrape.
func (e *Exporter) resetGauges() {
	for _, g := range e.allGaugeVecs() {
		g.Reset()
	}
	e.up.Reset()
	e.scrapeDuration.Reset()
}
