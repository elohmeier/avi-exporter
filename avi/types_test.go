package avi

import (
	"encoding/json"
	"testing"
)

func TestVSInventoryAcceptsObjectLastChangedTime(t *testing.T) {
	raw := []byte(`{
		"config": {
			"uuid": "virtualservice-1",
			"name": "vs-1",
			"enabled": true
		},
		"runtime": {
			"oper_status": {
				"state": "OPER_UP",
				"last_changed_time": {
					"secs": 1710000000,
					"usecs": 123
				}
			},
			"percent_ses_up": 100
		},
		"health_score": {
			"health_score": 100,
			"performance_score": {
				"application_performance": {
					"reason": "ok"
				}
			}
		}
	}`)

	var item VSInventoryItem
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatalf("unmarshal VS inventory with documented last_changed_time object: %v", err)
	}
	if item.Runtime.OperStatus.State != "OPER_UP" {
		t.Fatalf("oper status = %q, want OPER_UP", item.Runtime.OperStatus.State)
	}
}

func TestVsVipInventoryExtractsUUIDsFromRefFields(t *testing.T) {
	raw := []byte(`{
		"config": {
			"uuid": "vsvip-1",
			"name": "vip-1",
			"virtualservices": [
				{"ref": "https://controller/api/virtualservice/virtualservice-vs-1#vs-1"}
			],
			"vip": [
				{
					"vip_id": "1",
					"enabled": true,
					"ip_address": {"type": "V4", "addr": "192.0.2.10"}
				}
			]
		},
		"runtime": {
			"vip_id": "1",
			"oper_status": {"state": "OPER_UP"},
			"service_engine": [
				{"ref": "https://controller/api/serviceengine/se-1#se-1", "name": "se-1", "active_on_se": true}
			]
		}
	}`)

	var item VsVipInventoryItem
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatalf("unmarshal vsvip inventory with ref fields: %v", err)
	}
	if got := item.Config.VirtualServices[0].UUID; got != "virtualservice-vs-1" {
		t.Fatalf("virtual service UUID = %q, want virtualservice-vs-1", got)
	}
	if got := item.Runtime[0].ServiceEngine[0].UUID; got != "se-1" {
		t.Fatalf("service engine UUID = %q, want se-1", got)
	}
}

func TestReferenceUnmarshalVariantsAndErrors(t *testing.T) {
	var direct VsRef
	if err := json.Unmarshal([]byte(`{"uuid":"vs-direct","ref":"https://controller/api/virtualservice/ignored"}`), &direct); err != nil {
		t.Fatalf("unmarshal direct VS ref: %v", err)
	}
	if direct.UUID != "vs-direct" {
		t.Fatalf("direct VS ref UUID = %q", direct.UUID)
	}
	if err := json.Unmarshal([]byte(`"bad"`), &direct); err == nil {
		t.Fatalf("VsRef accepted invalid JSON type")
	}

	var se VipSeAssigned
	if err := json.Unmarshal([]byte(`{"url":"https://controller/api/serviceengine/se-url","uuid":"se-direct"}`), &se); err != nil {
		t.Fatalf("unmarshal direct SE ref: %v", err)
	}
	if se.URL != "https://controller/api/serviceengine/se-url" || se.UUID != "se-direct" {
		t.Fatalf("direct SE ref = %#v", se)
	}
	if err := json.Unmarshal([]byte(`"bad"`), &se); err == nil {
		t.Fatalf("VipSeAssigned accepted invalid JSON type")
	}
}

func TestVsVipInventoryRuntimeShapesAndErrors(t *testing.T) {
	var item VsVipInventoryItem
	if err := json.Unmarshal([]byte(`{"config":{"uuid":"vip-null","name":"vip-null"},"runtime":null}`), &item); err != nil {
		t.Fatalf("unmarshal null runtime: %v", err)
	}
	if len(item.Runtime) != 0 {
		t.Fatalf("null runtime length = %d, want 0", len(item.Runtime))
	}

	if err := json.Unmarshal([]byte(`{"config":{"uuid":"vip-array","name":"vip-array"},"runtime":[{"vip_id":"1","oper_status":{"state":"OPER_UP"}}]}`), &item); err != nil {
		t.Fatalf("unmarshal array runtime: %v", err)
	}
	if len(item.Runtime) != 1 || item.Runtime[0].VipID != "1" {
		t.Fatalf("array runtime = %#v", item.Runtime)
	}

	if err := json.Unmarshal([]byte(`"bad"`), &item); err == nil {
		t.Fatalf("VsVipInventoryItem accepted invalid JSON type")
	}
	if err := json.Unmarshal([]byte(`{"runtime":"bad"}`), &item); err == nil {
		t.Fatalf("VsVipInventoryItem accepted invalid runtime shape")
	}
}

func TestRefUUIDStripsIncludeNameSuffix(t *testing.T) {
	got := RefUUID("https://controller/api/pool/pool-1#pool-name")
	if got != "pool-1" {
		t.Fatalf("RefUUID returned %q, want pool-1", got)
	}
}
