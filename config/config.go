package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var validLabelName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config holds the exporter configuration.
type Config struct {
	Labels          map[string]string
	DisabledModules []string
	Tenants         []string // tenant names to scrape; "*" means all tenants visible at login, with /api/tenant fallback
	APIVersion      string   // X-Avi-Version header value; defaults to a recent supported version
	MetricsStep     int      // analytics step in seconds (default 300)
	MetricsLimit    int      // analytics limit (number of samples; default 1)
}

// IsModuleDisabled returns true if the given module name is in the disabled list.
func (c *Config) IsModuleDisabled(name string) bool {
	for _, m := range c.DisabledModules {
		if m == name {
			return true
		}
	}
	return false
}

// LabelKeys returns the sorted list of label keys.
func (c *Config) LabelKeys() []string {
	keys := make([]string, 0, len(c.Labels))
	for k := range c.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ValidateLabels returns an error when custom labels are not valid Prometheus
// label names or would collide with exporter-managed labels.
func ValidateLabels(labels map[string]string, reserved []string) error {
	reservedSet := make(map[string]bool, len(reserved))
	for _, label := range reserved {
		reservedSet[label] = true
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if !validLabelName.MatchString(key) {
			return fmt.Errorf("invalid label %q: label names must match [A-Za-z_][A-Za-z0-9_]*", key)
		}
		if strings.HasPrefix(key, "__") {
			return fmt.Errorf("invalid label %q: label names starting with __ are reserved", key)
		}
		if reservedSet[key] {
			return fmt.Errorf("invalid label %q: reserved exporter label", key)
		}
	}
	return nil
}

// GetCredentials reads credentials from environment variables.
func GetCredentials() (username, password string) {
	username = os.Getenv("AVI_USERNAME")
	password = os.Getenv("AVI_PASSWORD")
	return username, password
}

// GetIgnoreCert reads the ignore cert setting from environment variable.
func GetIgnoreCert() bool {
	val := strings.ToLower(os.Getenv("AVI_IGNORE_CERT"))
	return val == "true" || val == "1"
}

// GetCAFile reads the CA file path from environment variable.
func GetCAFile() string {
	return os.Getenv("AVI_CA_FILE")
}

// GetURL reads the controller URL from environment variable.
func GetURL() string {
	return os.Getenv("AVI_URL")
}

// GetLabels reads labels from environment variable.
func GetLabels() string {
	return os.Getenv("AVI_LABELS")
}

// GetDisabledModules reads disabled modules from environment variable.
func GetDisabledModules() string {
	return os.Getenv("AVI_DISABLED_MODULES")
}

// GetTenants reads the tenant list from environment variable.
func GetTenants() string {
	return os.Getenv("AVI_TENANTS")
}

// GetAPIVersion reads the X-Avi-Version header value from environment variable.
func GetAPIVersion() string {
	return os.Getenv("AVI_API_VERSION")
}

// ParseLabels parses a comma-separated key=value string into a map.
func ParseLabels(labelsStr string) map[string]string {
	labels := make(map[string]string)
	if labelsStr == "" {
		return labels
	}
	for _, pair := range strings.Split(labelsStr, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key != "" {
				labels[key] = value
			}
		}
	}
	return labels
}

// ParseCSV parses a comma-separated list, trimming whitespace and dropping empties.
func ParseCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
