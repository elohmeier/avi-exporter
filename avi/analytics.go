package avi

import (
	"context"
	"fmt"
)

// metric_entity values for the analytics collection API.
const (
	EntityVS         = "VSERVER_METRICS_ENTITY"
	EntitySE         = "SE_METRICS_ENTITY"
	EntityVM         = "VM_METRICS_ENTITY" // server / pool member
	EntityController = "CONTROLLER_METRICS_ENTITY"
)

// MetricQuery is one entry inside the analytics collection POST.
// metric_id is one ID per item in the POST form (unlike GET, where it's CSV).
type MetricQuery struct {
	EntityUUID        string `json:"entity_uuid,omitempty"` // "*" fans out across all entities of MetricEntity
	MetricEntity      string `json:"metric_entity,omitempty"`
	ObjID             string `json:"obj_id,omitempty"`
	MetricID          string `json:"metric_id"`
	Step              int    `json:"step,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	PoolUUID          string `json:"pool_uuid,omitempty"`
	ServiceEngineUUID string `json:"serviceengine_uuid,omitempty"`
	TenantUUID        string `json:"tenant_uuid,omitempty"`
	PadMissingData    *bool  `json:"pad_missing_data,omitempty"`
}

// MetricsCollectionRequest is the body of POST /api/analytics/metrics/collection.
type MetricsCollectionRequest struct {
	MetricRequests []MetricQuery `json:"metric_requests"`
}

// DataPoint is one timeseries sample.
type DataPoint struct {
	Timestamp string  `json:"timestamp"`
	Value     float64 `json:"value"`
}

// MetricSeries is the per-metric series in an analytics response.
type MetricSeries struct {
	Header struct {
		Name              string `json:"name"`
		EntityUUID        string `json:"entity_uuid"`
		TenantUUID        string `json:"tenant_uuid,omitempty"`
		Units             string `json:"units,omitempty"`
		ObjIDType         string `json:"obj_id_type,omitempty"`
		MetricDescription string `json:"metric_description,omitempty"`
		Statistics        *struct {
			Min  float64 `json:"min"`
			Max  float64 `json:"max"`
			Mean float64 `json:"mean"`
		} `json:"statistics,omitempty"`
	} `json:"header"`
	Data []DataPoint `json:"data"`
}

// MetricsCollectionResponse: the controller returns
//
//	{"series": {"<entity-uuid>": [{header,data}, ...], ...}}
//
// with one MetricSeries per metric_id queried for that entity.
type MetricsCollectionResponse struct {
	Series map[string][]MetricSeries `json:"series"`
}

// Last returns the last value in s.Data, or (0,false) if empty.
func (s MetricSeries) Last() (float64, bool) {
	if len(s.Data) == 0 {
		return 0, false
	}
	return s.Data[len(s.Data)-1].Value, true
}

// CollectMetrics posts a batch metrics-collection request.
func (c *Client) CollectMetrics(ctx context.Context, tenant string, req MetricsCollectionRequest) (*MetricsCollectionResponse, error) {
	if len(req.MetricRequests) == 0 {
		return &MetricsCollectionResponse{Series: map[string][]MetricSeries{}}, nil
	}
	var resp MetricsCollectionResponse
	err := c.Post(ctx, "/api/analytics/metrics/collection", &resp, RequestOptions{
		Tenant: tenant,
		Body:   req,
	})
	if err != nil {
		return nil, fmt.Errorf("collect metrics: %w", err)
	}
	if resp.Series == nil {
		resp.Series = map[string][]MetricSeries{}
	}
	return &resp, nil
}
