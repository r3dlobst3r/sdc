package jobs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/saltyorg/sdc/internal/orchestrator"
	"github.com/saltyorg/sdc/pkg/logger"
)

const (
	// DefaultWorkerCount is the number of concurrent workers
	DefaultWorkerCount = 3

	// MinJobRetention is the minimum time to keep completed jobs
	MinJobRetention = 1 * time.Hour

	// MaxJobCount is the maximum number of jobs to retain
	MaxJobCount = 1000

	// CleanupInterval is how often to run job cleanup
	CleanupInterval = 5 * time.Minute
)

// Manager manages job lifecycle and execution
type Manager struct {
	orchestrator *orchestrator.Orchestrator
	logger       *logger.Logger

	jobs      map[string]*Job
	jobsMu    sync.RWMutex
	jobQueue  chan *Job
	workers   int
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	cleanupWg sync.WaitGroup
}

// NewManager creates a new job manager
func NewManager(orch *orchestrator.Orchestrator, logger *logger.Logger, workers int) *Manager {
	if workers <= 0 {
		workers = DefaultWorkerCount
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		orchestrator: orch,
		logger:       logger,
		jobs:         make(map[string]*Job),
		jobQueue:     make(chan *Job, 100), // Buffered channel
		workers:      workers,
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start worker pool
	for i := 0; i < workers; i++ {
		m.wg.Add(1)
		go m.worker(i)
	}

	// Start cleanup goroutine
	m.cleanupWg.Add(1)
	go m.cleanupLoop()

	m.logger.Info("Job manager started",
		"workers", workers,
		"cleanup_interval", CleanupInterval)

	return m
}

// Shutdown gracefully stops the job manager
func (m *Manager) Shutdown(timeout time.Duration) error {
	m.logger.Info("Shutting down job manager")

	// Stop accepting new jobs
	close(m.jobQueue)

	// Cancel context to stop cleanup loop
	m.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Info("All workers stopped gracefully")
	case <-time.After(timeout):
		m.logger.Warn("Worker shutdown timeout exceeded")
	}

	// Wait for cleanup goroutine
	m.cleanupWg.Wait()

	return nil
}

// Submit submits a new job for execution
func (m *Manager) Submit(job *Job) error {
	// Check if shutting down first
	select {
	case <-m.ctx.Done():
		return fmt.Errorf("job manager is shutting down")
	default:
	}

	m.jobsMu.Lock()
	m.jobs[job.ID] = job
	m.jobsMu.Unlock()

	m.logger.Info("Job submitted",
		"job_id", job.ID,
		"type", string(job.Type))

	select {
	case m.jobQueue <- job:
		return nil
	case <-m.ctx.Done():
		return fmt.Errorf("job manager is shutting down")
	}
}

// Get retrieves a job by ID
func (m *Manager) Get(id string) (*Job, error) {
	m.jobsMu.RLock()
	defer m.jobsMu.RUnlock()

	job, exists := m.jobs[id]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", id)
	}

	return job.Clone(), nil
}

// List returns all jobs
func (m *Manager) List() []*Job {
	m.jobsMu.RLock()
	defer m.jobsMu.RUnlock()

	result := make([]*Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		result = append(result, job.Clone())
	}

	return result
}

// Delete removes a job by ID
func (m *Manager) Delete(id string) error {
	m.jobsMu.Lock()
	defer m.jobsMu.Unlock()

	if _, exists := m.jobs[id]; !exists {
		return fmt.Errorf("job not found: %s", id)
	}

	delete(m.jobs, id)
	m.logger.Debug("Job deleted", "job_id", id)
	return nil
}

// worker processes jobs from the queue
func (m *Manager) worker(id int) {
	defer m.wg.Done()

	m.logger.Debug("Worker started", "worker_id", id)

	for job := range m.jobQueue {
		m.processJob(job)
	}

	m.logger.Debug("Worker stopped", "worker_id", id)
}

// processJob executes a single job
func (m *Manager) processJob(job *Job) {
	job.SetStatus(JobStatusRunning)

	m.logger.Info("Processing job",
		"job_id", job.ID,
		"type", string(job.Type))

	ctx := context.Background()

	switch job.Type {
	case JobTypeStart:
		m.processStartJob(ctx, job)
	case JobTypeStop:
		m.processStopJob(ctx, job)
	default:
		job.SetError(fmt.Errorf("unknown job type: %s", job.Type))
	}

	m.logger.Info("Job completed",
		"job_id", job.ID,
		"status", string(job.GetStatus()),
		"duration", job.Duration())
}

// processStartJob handles container start operations
func (m *Manager) processStartJob(ctx context.Context, job *Job) {
	m.logger.Info("Processing start job",
		"job_id", job.ID,
		"timeout", job.Timeout)

	opts := orchestrator.StartContainersOptions{
		Timeout: job.Timeout,
		Ignore:  job.Ignore,
	}

	result, err := m.orchestrator.StartContainers(ctx, opts)
	if err != nil {
		job.SetError(err)
		m.logger.Error("Start job failed",
			"job_id", job.ID,
			"error", err)
		return
	}

	job.SetResults(result.Started, nil, result.Skipped, result.Failed)

	// If any containers failed, mark job as failed
	if len(result.Failed) > 0 {
		job.SetStatus(JobStatusFailed)
		if len(result.Started) == 0 && len(result.Skipped) == 0 {
			m.logger.Error("Start job failed - all containers failed to start",
				"job_id", job.ID,
				"failed", len(result.Failed))
		} else {
			totalContainers := len(result.Started) + len(result.Skipped) + len(result.Failed)
			m.logger.Error("Start job failed - some containers failed to start",
				"job_id", job.ID,
				"started", len(result.Started),
				"skipped", len(result.Skipped),
				"failed", len(result.Failed),
				"total", totalContainers)
		}
	} else {
		job.SetStatus(JobStatusCompleted)
		m.logger.Info("Start job completed",
			"job_id", job.ID,
			"started", len(result.Started),
			"skipped", len(result.Skipped),
			"failed", len(result.Failed))
	}
}

// processStopJob handles container stop operations
func (m *Manager) processStopJob(ctx context.Context, job *Job) {
	m.logger.Info("Processing stop job",
		"job_id", job.ID,
		"timeout", job.Timeout)

	opts := orchestrator.StopContainersOptions{
		Timeout: job.Timeout,
		Ignore:  job.Ignore,
	}

	result, err := m.orchestrator.StopContainers(ctx, opts)
	if err != nil {
		job.SetError(err)
		m.logger.Error("Stop job failed",
			"job_id", job.ID,
			"error", err)
		return
	}

	job.SetResults(nil, result.Stopped, result.Skipped, result.Failed)

	// If any containers failed, mark job as failed
	if len(result.Failed) > 0 {
		job.SetStatus(JobStatusFailed)
		if len(result.Stopped) == 0 && len(result.Skipped) == 0 {
			m.logger.Error("Stop job failed - all containers failed to stop",
				"job_id", job.ID,
				"failed", len(result.Failed))
		} else {
			totalContainers := len(result.Stopped) + len(result.Skipped) + len(result.Failed)
			m.logger.Error("Stop job failed - some containers failed to stop",
				"job_id", job.ID,
				"stopped", len(result.Stopped),
				"skipped", len(result.Skipped),
				"failed", len(result.Failed),
				"total", totalContainers)
		}
	} else {
		job.SetStatus(JobStatusCompleted)
		m.logger.Info("Stop job completed",
			"job_id", job.ID,
			"stopped", len(result.Stopped),
			"skipped", len(result.Skipped),
			"failed", len(result.Failed))
	}
}

// cleanupLoop periodically cleans up old jobs
func (m *Manager) cleanupLoop() {
	defer m.cleanupWg.Done()

	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.logger.Debug("Cleanup loop stopping")
			return
		case <-ticker.C:
			m.cleanup()
		}
	}
}

// cleanup removes old jobs based on retention policy
func (m *Manager) cleanup() {
	m.jobsMu.Lock()
	defer m.jobsMu.Unlock()

	now := time.Now()
	totalJobs := len(m.jobs)

	if totalJobs == 0 {
		return
	}

	// Collect jobs eligible for cleanup (completed/failed and older than MinJobRetention)
	type jobAge struct {
		id  string
		age time.Duration
	}

	var eligible []jobAge
	for id, job := range m.jobs {
		status := job.GetStatus()
		if status == JobStatusCompleted || status == JobStatusFailed {
			age := now.Sub(job.CreatedAt)
			if age > MinJobRetention {
				eligible = append(eligible, jobAge{id: id, age: age})
			}
		}
	}

	if len(eligible) == 0 && totalJobs <= MaxJobCount {
		return
	}

	// If we're over the max count, sort by age and remove oldest
	if totalJobs > MaxJobCount {
		// Sort eligible by age (oldest first)
		for i := 0; i < len(eligible); i++ {
			for j := i + 1; j < len(eligible); j++ {
				if eligible[j].age > eligible[i].age {
					eligible[i], eligible[j] = eligible[j], eligible[i]
				}
			}
		}

		// Remove enough jobs to get under MaxJobCount
		toRemove := min(totalJobs-MaxJobCount, len(eligible))

		removed := 0
		for i := range toRemove {
			delete(m.jobs, eligible[i].id)
			removed++
		}

		m.logger.Info("Cleaned up old jobs (LRU eviction)",
			"removed", removed,
			"remaining", len(m.jobs))
	} else if len(eligible) > 0 {
		// Remove old eligible jobs even if under MaxJobCount
		removed := 0
		for _, job := range eligible {
			delete(m.jobs, job.id)
			removed++
		}

		m.logger.Info("Cleaned up old jobs (age-based)",
			"removed", removed,
			"remaining", len(m.jobs))
	}
}
