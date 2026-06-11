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

func TestRefUUIDStripsIncludeNameSuffix(t *testing.T) {
	got := RefUUID("https://controller/api/pool/pool-1#pool-name")
	if got != "pool-1" {
		t.Fatalf("RefUUID returned %q, want pool-1", got)
	}
}
