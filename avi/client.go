// Package avi provides a Prometheus-friendly client for the Avi Load Balancer
// Controller REST API. Authentication mirrors the official alb-sdk Go session:
// POST /login captures csrftoken+sessionid cookies; subsequent requests carry
// the X-CSRFToken header, csrftoken+sessionid cookies, X-Avi-Version, and
// optionally X-Avi-Tenant. On 401/419 we transparently re-login once.
package avi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Client is a session-based Avi controller client.
type Client struct {
	baseURL    string // trimmed, no trailing slash, e.g. "https://avi.example.com"
	username   string
	password   string
	apiVersion string

	client *http.Client

	authMu    sync.Mutex
	csrfToken string
	sessionID string
	tenants   []Tenant

	logger *slog.Logger
}

type loginResponse struct {
	Tenants []Tenant `json:"tenants"`
}

// NewClient builds a session-based client. caFile and ignoreCert are mutually
// exclusive; ignoreCert wins. logger may be nil.
func NewClient(rawURL, username, password, apiVersion string, ignoreCert bool, caFile string, logger *slog.Logger) (*Client, error) {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     30 * time.Second,
	}

	if ignoreCert {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} else if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse CA certificate")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}

	return &Client{
		baseURL:    strings.TrimRight(strings.TrimSpace(rawURL), "/"),
		username:   username,
		password:   password,
		apiVersion: apiVersion,
		client: &http.Client{
			Timeout:   60 * time.Second,
			Transport: transport,
		},
		logger: logger,
	}, nil
}

// CloseIdleConnections closes pooled connections.
func (c *Client) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}

func (c *Client) hasSession() bool {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return c.sessionID != ""
}

func (c *Client) clearSession() {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	c.csrfToken = ""
	c.sessionID = ""
	c.tenants = nil
}

// Login posts to /login and captures the csrftoken + sessionid cookies.
// Safe to call repeatedly; no-op if a session is already active.
func (c *Client) Login(ctx context.Context) error {
	c.authMu.Lock()
	if c.sessionID != "" {
		c.authMu.Unlock()
		return nil
	}
	c.authMu.Unlock()

	payload := map[string]string{
		"username": c.username,
		"password": c.password,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/login", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", c.baseURL+"/")
	if c.apiVersion != "" {
		req.Header.Set("X-Avi-Version", c.apiVersion)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("login request: %w", err)
	}
	defer resp.Body.Close()
	rawBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("login failed: %s (%s)", resp.Status, truncate(string(rawBody), 200))
	}

	var csrf, sid string
	for _, ck := range resp.Cookies() {
		switch ck.Name {
		case "csrftoken":
			csrf = ck.Value
		case "sessionid", "avi-sessionid":
			sid = ck.Value
		}
	}
	if csrf == "" || sid == "" {
		return fmt.Errorf("login response missing csrftoken/sessionid cookies")
	}

	var login loginResponse
	if err := json.Unmarshal(rawBody, &login); err != nil && c.logger != nil {
		c.logger.Debug("could not parse avi login response", "err", err)
	}

	c.authMu.Lock()
	c.csrfToken = csrf
	c.sessionID = sid
	c.tenants = append([]Tenant{}, login.Tenants...)
	c.authMu.Unlock()

	if c.logger != nil {
		c.logger.Info("avi login successful", "url", c.baseURL)
	}
	return nil
}

// LoginTenants returns the tenant list included in the authenticated /login
// response. It is scoped to the logged-in user and does not require read
// access on the Tenant resource.
func (c *Client) LoginTenants(ctx context.Context) ([]Tenant, error) {
	if err := c.Login(ctx); err != nil {
		return nil, err
	}
	c.authMu.Lock()
	defer c.authMu.Unlock()
	return append([]Tenant{}, c.tenants...), nil
}

// RequestOptions controls per-request behavior.
type RequestOptions struct {
	Tenant string // X-Avi-Tenant override; empty means leave unset
	Query  url.Values
	Body   any // marshaled as JSON if non-nil
}

// Get fetches and unmarshals JSON into out.
func (c *Client) Get(ctx context.Context, path string, out any, opt RequestOptions) error {
	return c.do(ctx, "GET", path, out, opt, true)
}

// GetRaw fetches a non-JSON response body. It uses the same authenticated Avi
// session handling as Get.
func (c *Client) GetRaw(ctx context.Context, path string, opt RequestOptions) ([]byte, error) {
	return c.doRaw(ctx, http.MethodGet, path, opt, true, "application/json, */*;q=0.5")
}

// Post performs a POST request and unmarshals JSON into out.
func (c *Client) Post(ctx context.Context, path string, out any, opt RequestOptions) error {
	return c.do(ctx, "POST", path, out, opt, true)
}

func (c *Client) do(ctx context.Context, method, path string, out any, opt RequestOptions, retryOnAuth bool) error {
	raw, err := c.doRaw(ctx, method, path, opt, retryOnAuth, "application/json")
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return responseUnmarshalError(path, raw, err)
	}
	return nil
}

func (c *Client) doRaw(ctx context.Context, method, path string, opt RequestOptions, retryOnAuth bool, accept string) ([]byte, error) {
	if !c.hasSession() {
		if err := c.Login(ctx); err != nil {
			return nil, err
		}
	}

	fullURL := c.baseURL + path
	if len(opt.Query) > 0 {
		sep := "?"
		if strings.Contains(fullURL, "?") {
			sep = "&"
		}
		fullURL = fullURL + sep + opt.Query.Encode()
	}

	var bodyReader io.Reader
	if opt.Body != nil {
		raw, err := json.Marshal(opt.Body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if accept == "" {
		accept = "application/json"
	}
	req.Header.Set("Accept", accept)
	if opt.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiVersion != "" {
		req.Header.Set("X-Avi-Version", c.apiVersion)
	}
	req.Header.Set("Referer", c.baseURL+"/")
	if opt.Tenant != "" {
		req.Header.Set("X-Avi-Tenant", opt.Tenant)
	}

	c.authMu.Lock()
	csrf := c.csrfToken
	sid := c.sessionID
	c.authMu.Unlock()
	if csrf != "" {
		req.Header.Set("X-CSRFToken", csrf)
		req.AddCookie(&http.Cookie{Name: "csrftoken", Value: csrf})
	}
	if sid != "" {
		req.AddCookie(&http.Cookie{Name: "sessionid", Value: sid})
		req.AddCookie(&http.Cookie{Name: "avi-sessionid", Value: sid})
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// CSRF token rotates mid-session — pick up any refreshed cookies.
	c.refreshCookies(resp)

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == 419 {
		if retryOnAuth {
			if c.logger != nil {
				c.logger.Info("avi session expired, re-logging in", "url", c.baseURL, "status", resp.StatusCode)
			}
			c.clearSession()
			if err := c.Login(ctx); err != nil {
				return nil, fmt.Errorf("re-login: %w", err)
			}
			return c.doRaw(ctx, method, path, opt, false, accept)
		}
		return nil, fmt.Errorf("%s %s: %s after re-login", method, path, resp.Status)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %s (%s)", method, path, resp.Status, truncate(string(raw), 200))
	}

	return raw, nil
}

// refreshCookies updates csrfToken/sessionID from any Set-Cookie on resp.
// Avi rotates the CSRF token mid-session, so this must run on every response.
func (c *Client) refreshCookies(resp *http.Response) {
	var newCSRF, newSID string
	for _, ck := range resp.Cookies() {
		switch ck.Name {
		case "csrftoken":
			newCSRF = ck.Value
		case "sessionid", "avi-sessionid":
			newSID = ck.Value
		}
	}
	if newCSRF == "" && newSID == "" {
		return
	}
	c.authMu.Lock()
	if newCSRF != "" {
		c.csrfToken = newCSRF
	}
	if newSID != "" {
		c.sessionID = newSID
	}
	c.authMu.Unlock()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func responseUnmarshalError(path string, raw []byte, err error) error {
	offset := jsonErrorOffset(err)
	excerpt := responseExcerpt(raw, offset, 1000)
	if offset > 0 {
		return fmt.Errorf("unmarshal %s response: %w (response excerpt near byte %d: %q)", path, err, offset, excerpt)
	}
	return fmt.Errorf("unmarshal %s response: %w (response excerpt: %q)", path, err, excerpt)
}

func jsonErrorOffset(err error) int64 {
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return syntaxErr.Offset
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return typeErr.Offset
	}
	return 0
}

func responseExcerpt(raw []byte, offset int64, limit int) string {
	if len(raw) <= limit || limit <= 0 {
		return string(raw)
	}

	start := 0
	if offset > 0 {
		start = int(offset) - 1 - limit/3
		if start < 0 {
			start = 0
		}
		if start+limit > len(raw) {
			start = len(raw) - limit
		}
	}

	end := start + limit
	excerpt := string(raw[start:end])
	if start > 0 {
		excerpt = "..." + excerpt
	}
	if end < len(raw) {
		excerpt += "..."
	}
	return excerpt
}
