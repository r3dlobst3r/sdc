package orchestrator

import (
	"testing"

	"github.com/r3dlobst3r/sdc/internal/docker"
	"github.com/r3dlobst3r/sdc/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{} // Mock client

	orch := New(dockerClient, log)

	assert.NotNil(t, orch)
	assert.NotNil(t, orch.docker)
	assert.NotNil(t, orch.builder)
	assert.NotNil(t, orch.logger)
}

func TestStartContainersOptions(t *testing.T) {
	opts := StartContainersOptions{
		Timeout: 600,
		Ignore:  []string{"traefik", "nginx"},
	}

	assert.Equal(t, 600, opts.Timeout)
	assert.Len(t, opts.Ignore, 2)
	assert.Contains(t, opts.Ignore, "traefik")
	assert.Contains(t, opts.Ignore, "nginx")
}

func TestStopContainersOptions(t *testing.T) {
	opts := StopContainersOptions{
		Timeout: 300,
		Ignore:  []string{"autoheal"},
	}

	assert.Equal(t, 300, opts.Timeout)
	assert.Len(t, opts.Ignore, 1)
	assert.Contains(t, opts.Ignore, "autoheal")
}

func TestStartResult(t *testing.T) {
	result := &StartResult{
		Started: []string{"nginx", "redis"},
		Skipped: []string{"traefik"},
		Failed:  []string{"broken"},
	}

	assert.Len(t, result.Started, 2)
	assert.Len(t, result.Skipped, 1)
	assert.Len(t, result.Failed, 1)
}

func TestStopResult(t *testing.T) {
	result := &StopResult{
		Stopped: []string{"app", "db"},
		Skipped: []string{"proxy"},
		Failed:  []string{},
	}

	assert.Len(t, result.Stopped, 2)
	assert.Len(t, result.Skipped, 1)
	assert.Len(t, result.Failed, 0)
}

// Note: Integration tests with actual Docker API would require:
// 1. Running Docker daemon
// 2. Test containers with proper labels
// 3. Mock Docker client or use testcontainers
//
// For now, we verify the basic structure and types compile correctly.
// Full integration testing will be done in Phase 4 (E2E tests).
