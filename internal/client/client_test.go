package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/r3dlobst3r/sdc/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	log, _ := logger.New(true)
	client := NewClient("http://localhost:3377", log)

	assert.NotNil(t, client)
	assert.Equal(t, "http://localhost:3377", client.baseURL)
	assert.NotNil(t, client.httpClient)
	assert.NotNil(t, client.logger)
}

func TestClient_StartContainers(t *testing.T) {
	log, _ := logger.New(true)

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/start", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		var req JobRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		assert.NoError(t, err)
		assert.Equal(t, 600, req.Timeout)
		assert.Equal(t, []string{"traefik"}, req.Ignore)

		resp := JobResponse{
			ID:     "test-job-id",
			Status: "pending",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	resp, err := client.StartContainers(ctx, 600, []string{"traefik"})
	assert.NoError(t, err)
	assert.Equal(t, "test-job-id", resp.ID)
	assert.Equal(t, "pending", resp.Status)
}

func TestClient_StopContainers(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/stop", r.URL.Path)
		assert.Equal(t, "POST", r.Method)

		var req JobRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		assert.NoError(t, err)
		assert.Equal(t, 300, req.Timeout)

		resp := JobResponse{
			ID:     "stop-job-id",
			Status: "pending",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	resp, err := client.StopContainers(ctx, 300, nil)
	assert.NoError(t, err)
	assert.Equal(t, "stop-job-id", resp.ID)
	assert.Equal(t, "pending", resp.Status)
}

func TestClient_GetJob(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/job_status/test-job-id", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		job := Job{
			ID:      "test-job-id",
			Type:    "start",
			Status:  "completed",
			Timeout: 600,
			Started: []string{"nginx", "redis"},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	job, err := client.GetJob(ctx, "test-job-id")
	assert.NoError(t, err)
	assert.Equal(t, "test-job-id", job.ID)
	assert.Equal(t, "start", job.Type)
	assert.Equal(t, "completed", job.Status)
	assert.Len(t, job.Started, 2)
}

func TestClient_WaitForJob(t *testing.T) {
	log, _ := logger.New(true)

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		var status string
		if callCount < 3 {
			status = "running"
		} else {
			status = "completed"
		}

		job := Job{
			ID:     "test-job-id",
			Type:   "start",
			Status: status,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	job, err := client.WaitForJob(ctx, "test-job-id", 100*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, "completed", job.Status)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestClient_WaitForJob_Timeout(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always return running
		job := Job{
			ID:     "test-job-id",
			Status: "running",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := client.WaitForJob(ctx, "test-job-id", 50*time.Millisecond)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func TestClient_Health(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ping", r.URL.Path)
		assert.Equal(t, "GET", r.Method)

		resp := HealthResponse{Status: "healthy"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	err := client.Health(ctx)
	assert.NoError(t, err)
}

func TestClient_Health_Unhealthy(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := HealthResponse{Status: "unhealthy"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	err := client.Health(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not healthy")
}

func TestClient_WaitForServerReady(t *testing.T) {
	log, _ := logger.New(true)

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		// Become healthy after 2 calls
		if callCount >= 2 {
			resp := HealthResponse{Status: "healthy"}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	err := client.WaitForServerReady(ctx, 5*time.Second)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestClient_WaitForServerReady_Timeout(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never become healthy
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	err := client.WaitForServerReady(ctx, 500*time.Millisecond)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestClient_Post_ErrorResponse(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "invalid request"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	_, err := client.StartContainers(ctx, 600, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestClient_Get_ErrorResponse(t *testing.T) {
	log, _ := logger.New(true)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "job not found"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, log)
	ctx := context.Background()

	_, err := client.GetJob(ctx, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "404")
}
