package docker

import (
	"testing"
	"time"

	"github.com/r3dlobst3r/sdc/pkg/logger"
)

func TestNewClient(t *testing.T) {
	log, err := logger.New(false)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	client, err := New("", log)
	if err != nil {
		t.Skipf("Docker daemon not available: %v", err)
	}
	defer client.Close()

	if client.cli == nil {
		t.Error("expected client to be initialized")
	}
}

func TestNewClientWithCustomConfig(t *testing.T) {
	log, err := logger.New(false)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	// Use a short timeout and fewer retries for faster testing
	cfg := ConnectionConfig{
		MaxRetries:     2,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		Timeout:        2 * time.Second,
	}

	client, err := NewWithConfig("", log, cfg)
	if err != nil {
		t.Skipf("Docker daemon not available: %v", err)
	}
	defer client.Close()

	if client.cli == nil {
		t.Error("expected client to be initialized")
	}
}

func TestNewClient_InvalidHost(t *testing.T) {
	log, err := logger.New(false)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	// Use a short configuration for faster testing
	cfg := ConnectionConfig{
		MaxRetries:     2,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Timeout:        500 * time.Millisecond,
	}

	// Try to connect to an invalid Docker socket
	client, err := NewWithConfig("unix:///nonexistent/docker.sock", log, cfg)
	if err == nil {
		client.Close()
		t.Error("expected error when connecting to invalid Docker socket")
	}

	if client != nil {
		t.Error("expected client to be nil on error")
	}
}

func TestDefaultConnectionConfig(t *testing.T) {
	cfg := DefaultConnectionConfig()

	if cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", cfg.MaxRetries)
	}

	if cfg.InitialBackoff != 1*time.Second {
		t.Errorf("expected InitialBackoff=1s, got %v", cfg.InitialBackoff)
	}

	if cfg.MaxBackoff != 30*time.Second {
		t.Errorf("expected MaxBackoff=30s, got %v", cfg.MaxBackoff)
	}

	if cfg.Timeout != 10*time.Second {
		t.Errorf("expected Timeout=10s, got %v", cfg.Timeout)
	}
}
