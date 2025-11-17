package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/saltyorg/sdc/pkg/logger"
)

// Client represents an HTTP client for communicating with the controller server
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *logger.Logger
	userAgent  string
}

// NewClient creates a new controller client
func NewClient(baseURL string, logger *logger.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger:    logger,
		userAgent: "sdc-client/1.0",
	}
}

// SetUserAgent sets a custom User-Agent header for requests
func (c *Client) SetUserAgent(userAgent string) {
	c.userAgent = userAgent
}

// JobRequest represents a request to start or stop containers
type JobRequest struct {
	Timeout int      `json:"timeout"`
	Ignore  []string `json:"ignore,omitempty"`
}

// JobResponse represents a job creation response
type JobResponse struct {
	ID     string `json:"job_id"`
	Status string `json:"status"`
}

// Job represents a job's full state
type Job struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Timeout   int       `json:"timeout"`
	Ignore    []string  `json:"ignore,omitempty"`
	Started   []string  `json:"started,omitempty"`
	Stopped   []string  `json:"stopped,omitempty"`
	Skipped   []string  `json:"skipped,omitempty"`
	Failed    []string  `json:"failed,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status string `json:"status"`
}

// StartContainers submits a job to start containers
func (c *Client) StartContainers(ctx context.Context, timeout int, ignore []string) (*JobResponse, error) {
	req := JobRequest{
		Timeout: timeout,
		Ignore:  ignore,
	}

	var resp JobResponse
	if err := c.post(ctx, "/start", req, &resp); err != nil {
		return nil, err
	}

	c.logger.Info("Start job submitted",
		"job_id", resp.ID,
		"status", resp.Status)

	return &resp, nil
}

// StopContainers submits a job to stop containers
func (c *Client) StopContainers(ctx context.Context, timeout int, ignore []string) (*JobResponse, error) {
	req := JobRequest{
		Timeout: timeout,
		Ignore:  ignore,
	}

	var resp JobResponse
	if err := c.post(ctx, "/stop", req, &resp); err != nil {
		return nil, err
	}

	c.logger.Info("Stop job submitted",
		"job_id", resp.ID,
		"status", resp.Status)

	return &resp, nil
}

// GetJob retrieves job status and results
func (c *Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var job Job
	if err := c.get(ctx, fmt.Sprintf("/job_status/%s", jobID), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// WaitForJob waits for a job to complete (completed or failed status)
func (c *Client) WaitForJob(ctx context.Context, jobID string, pollInterval time.Duration) (*Job, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			job, err := c.GetJob(ctx, jobID)
			if err != nil {
				return nil, fmt.Errorf("failed to get job status: %w", err)
			}

			c.logger.Debug("Job status",
				"job_id", jobID,
				"status", job.Status)

			if job.Status == "completed" || job.Status == "failed" {
				return job, nil
			}
		}
	}
}

// Health checks if the server is healthy
func (c *Client) Health(ctx context.Context) error {
	var resp HealthResponse
	if err := c.get(ctx, "/ping", &resp); err != nil {
		return err
	}

	if resp.Status != "healthy" {
		return fmt.Errorf("server is not healthy: %s", resp.Status)
	}

	return nil
}

// WaitForServerReady waits for the server to become ready
func (c *Client) WaitForServerReady(ctx context.Context, timeout time.Duration) error {
	c.logger.Info("Waiting for server to become ready",
		"url", c.baseURL,
		"timeout", timeout)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for server to become ready")
			}

			if err := c.Health(ctx); err != nil {
				c.logger.Debug("Server not ready yet",
					"error", err,
					"remaining", time.Until(deadline))
				continue
			}

			c.logger.Info("Server is ready")
			return nil
		}
	}
}

// post performs a POST request
func (c *Client) post(ctx context.Context, path string, body any, result any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

// get performs a GET request
func (c *Client) get(ctx context.Context, path string, result any) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}
