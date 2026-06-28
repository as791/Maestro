package maestro

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

type Option func(*Client)

func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.httpClient = c } }
func WithToken(t string) Option            { return func(cl *Client) { cl.token = t } }

func NewClient(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) Deployment(env, ns, name string) *Deployment {
	return &Deployment{client: c, Env: env, Namespace: ns, Name: name}
}

func (c *Client) ListDeployments(ctx context.Context, opts ListOptions) (*DeploymentList, error) {
	params := url.Values{}
	if opts.Environment != "" {
		params.Set("environment", opts.Environment)
	}
	if opts.Namespace != "" {
		params.Set("namespace", opts.Namespace)
	}
	if opts.Limit > 0 {
		params.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.PageToken != "" {
		params.Set("pageToken", opts.PageToken)
	}
	var result DeploymentList
	if err := c.get(ctx, "/api/v1/deployments?"+params.Encode(), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) Summary(ctx context.Context) ([]DeploymentCardSummary, error) {
	var result []DeploymentCardSummary
	if err := c.get(ctx, "/api/v1/deployments/summary", &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) ClusterState(ctx context.Context, env, ns string) (*ClusterActorState, error) {
	var result ClusterActorState
	if err := c.get(ctx, fmt.Sprintf("/api/v1/clusters/%s/%s/actor", env, ns), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) FreezeCluster(ctx context.Context, env, ns string, req ClusterCommandRequest) error {
	return c.postMutation(ctx, fmt.Sprintf("/api/v1/clusters/%s/%s/freeze", env, ns), req, "", nil)
}

func (c *Client) UnfreezeCluster(ctx context.Context, env, ns string, req ClusterCommandRequest) error {
	return c.postMutation(ctx, fmt.Sprintf("/api/v1/clusters/%s/%s/unfreeze", env, ns), req, "", nil)
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	return c.do(req, out)
}

func (c *Client) postMutation(ctx context.Context, path string, body any, idempotencyKey string, out any) error {
	if idempotencyKey == "" {
		idempotencyKey = generateKey()
	}
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Idempotency-Key", idempotencyKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req, out)
}

func (c *Client) putJSON(ctx context.Context, path string, body any, out any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.do(req, out)
}

func (c *Client) do(req *http.Request, out any) error {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var errBody struct{ Error string `json:"error"` }
		_ = json.Unmarshal(data, &errBody)
		msg := errBody.Error
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return &APIError{StatusCode: resp.StatusCode, Message: msg}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func generateKey() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixMilli(), hex.EncodeToString(b))
}
