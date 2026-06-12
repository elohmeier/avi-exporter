package config

import (
	"reflect"
	"testing"
)

func TestConfigHelpers(t *testing.T) {
	cfg := &Config{
		Labels: map[string]string{
			"zone": "eu",
			"env":  "prod",
		},
		DisabledModules: []string{"vs_metrics", "gslb"},
	}

	if !cfg.IsModuleDisabled("gslb") {
		t.Fatalf("IsModuleDisabled(gslb) = false, want true")
	}
	if cfg.IsModuleDisabled("pool_metrics") {
		t.Fatalf("IsModuleDisabled(pool_metrics) = true, want false")
	}

	if got, want := cfg.LabelKeys(), []string{"env", "zone"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LabelKeys() = %#v, want %#v", got, want)
	}
}

func TestEnvironmentGetters(t *testing.T) {
	t.Setenv("AVI_USERNAME", "admin")
	t.Setenv("AVI_PASSWORD", "secret")
	t.Setenv("AVI_IGNORE_CERT", "true")
	t.Setenv("AVI_CA_FILE", "/tmp/ca.pem")
	t.Setenv("AVI_URL", "https://avi.example.com")
	t.Setenv("AVI_LABELS", "env=prod")
	t.Setenv("AVI_DISABLED_MODULES", "gslb")
	t.Setenv("AVI_TENANTS", "admin,*")
	t.Setenv("AVI_API_VERSION", "30.2.1")

	user, pass := GetCredentials()
	if user != "admin" || pass != "secret" {
		t.Fatalf("GetCredentials() = %q, %q; want admin, secret", user, pass)
	}
	if !GetIgnoreCert() {
		t.Fatalf("GetIgnoreCert() = false, want true")
	}
	if got := GetCAFile(); got != "/tmp/ca.pem" {
		t.Fatalf("GetCAFile() = %q", got)
	}
	if got := GetURL(); got != "https://avi.example.com" {
		t.Fatalf("GetURL() = %q", got)
	}
	if got := GetLabels(); got != "env=prod" {
		t.Fatalf("GetLabels() = %q", got)
	}
	if got := GetDisabledModules(); got != "gslb" {
		t.Fatalf("GetDisabledModules() = %q", got)
	}
	if got := GetTenants(); got != "admin,*" {
		t.Fatalf("GetTenants() = %q", got)
	}
	if got := GetAPIVersion(); got != "30.2.1" {
		t.Fatalf("GetAPIVersion() = %q", got)
	}

	t.Setenv("AVI_IGNORE_CERT", "1")
	if !GetIgnoreCert() {
		t.Fatalf("GetIgnoreCert() with 1 = false, want true")
	}
	t.Setenv("AVI_IGNORE_CERT", "false")
	if GetIgnoreCert() {
		t.Fatalf("GetIgnoreCert() with false = true, want false")
	}
}

func TestParseLabels(t *testing.T) {
	got := ParseLabels(" env = prod , ignored , site= eu-west , empty= , =bad ,,")
	want := map[string]string{
		"env":   "prod",
		"site":  "eu-west",
		"empty": "",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseLabels() = %#v, want %#v", got, want)
	}
	if got := ParseLabels(""); len(got) != 0 {
		t.Fatalf("ParseLabels(empty) = %#v, want empty map", got)
	}
}

func TestParseCSV(t *testing.T) {
	if got := ParseCSV(""); got != nil {
		t.Fatalf("ParseCSV(empty) = %#v, want nil", got)
	}
	got := ParseCSV(" admin, *, ,tenant-a ")
	want := []string{"admin", "*", "tenant-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseCSV() = %#v, want %#v", got, want)
	}
}

func TestValidateLabels(t *testing.T) {
	if err := ValidateLabels(map[string]string{"env": "prod", "site_1": "eu"}, []string{"tenant"}); err != nil {
		t.Fatalf("ValidateLabels(valid) = %v", err)
	}

	tests := []struct {
		name     string
		labels   map[string]string
		reserved []string
	}{
		{
			name:   "invalid name",
			labels: map[string]string{"bad-label": "value"},
		},
		{
			name:   "prometheus reserved prefix",
			labels: map[string]string{"__name__": "value"},
		},
		{
			name:     "exporter reserved label",
			labels:   map[string]string{"tenant": "override"},
			reserved: []string{"tenant"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateLabels(tt.labels, tt.reserved); err == nil {
				t.Fatalf("ValidateLabels(%s) succeeded, want error", tt.name)
			}
		})
	}
}
