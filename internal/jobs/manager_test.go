package jobs

import (
	"testing"
	"time"

	"github.com/r3dlobst3r/sdc/internal/docker"
	"github.com/r3dlobst3r/sdc/internal/orchestrator"
	"github.com/r3dlobst3r/sdc/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestNewManager(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{} // Mock
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 2)
	defer mgr.Shutdown(5 * time.Second)

	assert.NotNil(t, mgr)
	assert.Equal(t, 2, mgr.workers)
	assert.NotNil(t, mgr.jobs)
	assert.NotNil(t, mgr.jobQueue)
}

func TestManager_SubmitAndGet(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	job := NewJob(JobTypeStart, 600, []string{"traefik"})

	// Add job directly to manager's jobs map instead of submitting to avoid worker execution
	mgr.jobsMu.Lock()
	mgr.jobs[job.ID] = job
	mgr.jobsMu.Unlock()

	retrieved, err := mgr.Get(job.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, retrieved.ID)
	assert.Equal(t, job.Type, retrieved.Type)
}

func TestManager_Get_NotFound(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	_, err := mgr.Get("non-existent-id")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job not found")
}

func TestManager_List(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	// Add multiple jobs directly to avoid worker execution
	job1 := NewJob(JobTypeStart, 600, nil)
	job2 := NewJob(JobTypeStop, 300, nil)

	mgr.jobsMu.Lock()
	mgr.jobs[job1.ID] = job1
	mgr.jobs[job2.ID] = job2
	mgr.jobsMu.Unlock()

	jobs := mgr.List()
	assert.Len(t, jobs, 2)

	ids := make(map[string]bool)
	for _, job := range jobs {
		ids[job.ID] = true
	}

	assert.True(t, ids[job1.ID])
	assert.True(t, ids[job2.ID])
}

func TestManager_Delete(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	job := NewJob(JobTypeStart, 600, nil)

	// Add job directly to avoid worker execution
	mgr.jobsMu.Lock()
	mgr.jobs[job.ID] = job
	mgr.jobsMu.Unlock()

	// Delete the job
	err := mgr.Delete(job.ID)
	assert.NoError(t, err)

	// Should not be found
	_, err = mgr.Get(job.ID)
	assert.Error(t, err)
}

func TestManager_Delete_NotFound(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	err := mgr.Delete("non-existent-id")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job not found")
}

func TestManager_Shutdown(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 2)

	// Shutdown should complete within timeout
	err := mgr.Shutdown(5 * time.Second)
	assert.NoError(t, err)

	// Should not accept new jobs after shutdown
	job := NewJob(JobTypeStart, 600, nil)
	err = mgr.Submit(job)
	assert.Error(t, err)
}

// Note: Integration tests for processStartJob and processStopJob require:
// 1. Running Docker daemon
// 2. Initialized Docker client
// 3. Test containers with proper labels
//
// These are tested in the orchestrator package.
// Here we verify job manager behavior with job submission and lifecycle.

func TestManager_Cleanup(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	// Add jobs with different ages
	oldJob := NewJob(JobTypeStart, 600, nil)
	oldJob.CreatedAt = time.Now().Add(-2 * time.Hour) // Old job
	oldJob.SetStatus(JobStatusCompleted)

	recentJob := NewJob(JobTypeStart, 600, nil)
	recentJob.SetStatus(JobStatusCompleted)

	mgr.jobsMu.Lock()
	mgr.jobs[oldJob.ID] = oldJob
	mgr.jobs[recentJob.ID] = recentJob
	mgr.jobsMu.Unlock()

	// Run cleanup
	mgr.cleanup()

	// Old job should be removed, recent job should remain
	mgr.jobsMu.RLock()
	_, oldExists := mgr.jobs[oldJob.ID]
	_, recentExists := mgr.jobs[recentJob.ID]
	mgr.jobsMu.RUnlock()

	assert.False(t, oldExists, "Old job should be cleaned up")
	assert.True(t, recentExists, "Recent job should be retained")
}

func TestManager_Cleanup_MaxJobCount(t *testing.T) {
	log, _ := logger.New(true)
	dockerClient := &docker.Client{}
	orch := orchestrator.New(dockerClient, log)

	mgr := NewManager(orch, log, 1)
	defer mgr.Shutdown(5 * time.Second)

	// Add more than MaxJobCount old jobs
	mgr.jobsMu.Lock()
	for range MaxJobCount + 10 {
		job := NewJob(JobTypeStart, 600, nil)
		job.CreatedAt = time.Now().Add(-2 * time.Hour)
		job.SetStatus(JobStatusCompleted)
		mgr.jobs[job.ID] = job
	}
	initialCount := len(mgr.jobs)
	mgr.jobsMu.Unlock()

	// Run cleanup
	mgr.cleanup()

	// Should remove excess jobs
	mgr.jobsMu.RLock()
	finalCount := len(mgr.jobs)
	mgr.jobsMu.RUnlock()

	assert.Less(t, finalCount, initialCount, "Should remove some jobs")
	assert.LessOrEqual(t, finalCount, MaxJobCount, "Should be under MaxJobCount")
}
