package orchestrator

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/saltyorg/sdc/internal/docker"
	"github.com/saltyorg/sdc/internal/graph"
	"github.com/saltyorg/sdc/pkg/logger"
)

// Orchestrator manages container lifecycle operations with dependency awareness
type Orchestrator struct {
	docker      *docker.Client
	builder     *graph.Builder
	logger      *logger.Logger
	maxParallel int
}

// New creates a new orchestrator instance
func New(dockerClient *docker.Client, logger *logger.Logger) *Orchestrator {
	parallel := max(runtime.GOMAXPROCS(0), 1)

	return &Orchestrator{
		docker:      dockerClient,
		builder:     graph.NewBuilder(dockerClient, logger),
		logger:      logger,
		maxParallel: parallel,
	}
}

// StartContainersOptions configures container startup behavior
type StartContainersOptions struct {
	Timeout int      // Operation timeout in seconds
	Ignore  []string // Container names to skip
}

// StopContainersOptions configures container shutdown behavior
type StopContainersOptions struct {
	Timeout int      // Operation timeout in seconds
	Ignore  []string // Container names to skip
}

// StartResult contains the results of a start operation
type StartResult struct {
	Started []string // Names of containers that were started
	Skipped []string // Names of containers that were skipped
	Failed  []string // Names of containers that failed to start
}

// StopResult contains the results of a stop operation
type StopResult struct {
	Stopped []string // Names of containers that were stopped
	Skipped []string // Names of containers that were skipped
	Failed  []string // Names of containers that failed to stop
}

// StartContainers starts all managed containers in dependency order
func (o *Orchestrator) StartContainers(ctx context.Context, opts StartContainersOptions) (*StartResult, error) {
	o.logger.Info("Starting container orchestration",
		"timeout", opts.Timeout,
		"ignore", opts.Ignore)

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(opts.Timeout)*time.Second)
	defer cancel()

	// List all containers
	containers, err := o.docker.ListManagedContainers(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	o.logger.Info("Found managed containers", "count", len(containers))

	// Build dependency graph
	g, err := o.builder.Build(timeoutCtx, containers)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// Get connected components for parallel execution
	components, err := g.GetConnectedComponents()
	if err != nil {
		return nil, fmt.Errorf("failed to identify connected components: %w", err)
	}

	o.logger.Info("Identified connected components",
		"component_count", len(components))

	// Create ignore map for fast lookup
	ignoreMap := make(map[string]bool)
	for _, name := range opts.Ignore {
		ignoreMap[name] = true
	}

	// Process components with bounded parallelism
	type componentWork struct {
		idx  int
		comp *graph.ComponentBatches
	}

	type componentResult struct {
		started []string
		skipped []string
		failed  []string
	}

	resultChan := make(chan componentResult, len(components))
	workChan := make(chan componentWork)

	workerCount := min(len(components), o.maxParallel)
	for range workerCount {
		go func() {
			for work := range workChan {
				compIndex := work.idx
				comp := work.comp

				compResult := componentResult{
					started: []string{},
					skipped: []string{},
					failed:  []string{},
				}

				// Get container names for this component
				var containerNames []string
				for _, batch := range comp.Batches {
					for _, node := range batch {
						containerNames = append(containerNames, node.Name)
					}
				}

				// Only log multi-container components at INFO level
				if len(containerNames) > 1 {
					o.logger.Info("Processing component",
						"containers", containerNames,
						"batch_count", len(comp.Batches))
				} else {
					o.logger.Debug("Processing component",
						"containers", containerNames,
						"batch_count", len(comp.Batches))
				}

				// Process batches sequentially (respecting dependencies between batches)
				for batchIdx, batch := range comp.Batches {
					if len(batch) == 0 {
						continue
					}

					batchIndex := batchIdx
					o.logger.Debug("Processing batch within component",
						"component", compIndex,
						"batch", batchIndex,
						"containers", len(batch))

					// Process containers in this batch with bounded parallelism
					type batchResult struct {
						started []string
						skipped []string
						failed  []string
					}

					jobChan := make(chan *graph.Node)
					batchChan := make(chan batchResult, len(batch))

					batchWorkers := min(len(batch), o.maxParallel)
					for range batchWorkers {
						go func() {
							for n := range jobChan {
								br := batchResult{
									started: []string{},
									skipped: []string{},
									failed:  []string{},
								}

								if ignoreMap[n.Name] {
									br.skipped = append(br.skipped, n.Name)
								} else if err := o.startContainer(timeoutCtx, n); err != nil {
									o.logger.Error("Failed to start container",
										"container", n.Name,
										"component", compIndex,
										"batch", batchIndex,
										"error", err)
									br.failed = append(br.failed, n.Name)
								} else {
									br.started = append(br.started, n.Name)
								}

								batchChan <- br
							}
						}()
					}

					for _, node := range batch {
						jobChan <- node
					}
					close(jobChan)

					// Collect results from this batch
					for range batch {
						br := <-batchChan
						compResult.started = append(compResult.started, br.started...)
						compResult.skipped = append(compResult.skipped, br.skipped...)
						compResult.failed = append(compResult.failed, br.failed...)
					}
				}

				resultChan <- compResult
			}
		}()
	}

	for componentIdx, component := range components {
		workChan <- componentWork{idx: componentIdx, comp: component}
	}
	close(workChan)

	// Collect results from all components
	result := &StartResult{
		Started: []string{},
		Skipped: []string{},
		Failed:  []string{},
	}

	for range components {
		compResult := <-resultChan
		result.Started = append(result.Started, compResult.started...)
		result.Skipped = append(result.Skipped, compResult.skipped...)
		result.Failed = append(result.Failed, compResult.failed...)
	}

	o.logger.Info("Container startup complete",
		"started", len(result.Started),
		"skipped", len(result.Skipped),
		"failed", len(result.Failed))

	return result, nil
}

// StopContainers stops all managed containers in reverse dependency order
func (o *Orchestrator) StopContainers(ctx context.Context, opts StopContainersOptions) (*StopResult, error) {
	o.logger.Info("Stopping container orchestration",
		"timeout", opts.Timeout,
		"ignore", opts.Ignore)

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(opts.Timeout)*time.Second)
	defer cancel()

	// List all containers
	containers, err := o.docker.ListManagedContainers(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	o.logger.Info("Found managed containers", "count", len(containers))

	// Build dependency graph
	g, err := o.builder.Build(timeoutCtx, containers)
	if err != nil {
		return nil, fmt.Errorf("failed to build dependency graph: %w", err)
	}

	// Get connected components for parallel execution (in shutdown order)
	components, err := g.GetConnectedComponentsForShutdown()
	if err != nil {
		return nil, fmt.Errorf("failed to identify connected components: %w", err)
	}

	o.logger.Info("Identified connected components for shutdown",
		"component_count", len(components))

	// Create ignore map for fast lookup
	ignoreMap := make(map[string]bool)
	for _, name := range opts.Ignore {
		ignoreMap[name] = true
	}

	// Process components with bounded parallelism
	type componentWork struct {
		idx  int
		comp *graph.ComponentBatches
	}

	type componentResult struct {
		stopped []string
		skipped []string
		failed  []string
	}

	resultChan := make(chan componentResult, len(components))
	workChan := make(chan componentWork)

	workerCount := min(len(components), o.maxParallel)
	for range workerCount {
		go func() {
			for work := range workChan {
				compIndex := work.idx
				comp := work.comp

				compResult := componentResult{
					stopped: []string{},
					skipped: []string{},
					failed:  []string{},
				}

				// Get container names for this component
				var containerNames []string
				for _, batch := range comp.Batches {
					for _, node := range batch {
						containerNames = append(containerNames, node.Name)
					}
				}

				// Only log multi-container components at INFO level
				if len(containerNames) > 1 {
					o.logger.Info("Processing shutdown component",
						"containers", containerNames,
						"batch_count", len(comp.Batches))
				} else {
					o.logger.Debug("Processing shutdown component",
						"containers", containerNames,
						"batch_count", len(comp.Batches))
				}

				// Process batches sequentially (respecting dependencies between batches)
				for batchIdx, batch := range comp.Batches {
					if len(batch) == 0 {
						continue
					}

					batchIndex := batchIdx
					o.logger.Debug("Processing batch within component",
						"component", compIndex,
						"batch", batchIndex,
						"containers", len(batch))

					// Process containers in this batch with bounded parallelism
					type batchResult struct {
						stopped []string
						skipped []string
						failed  []string
					}

					jobChan := make(chan *graph.Node)
					batchChan := make(chan batchResult, len(batch))

					batchWorkers := min(len(batch), o.maxParallel)
					for range batchWorkers {
						go func() {
							for n := range jobChan {
								br := batchResult{
									stopped: []string{},
									skipped: []string{},
									failed:  []string{},
								}

								if ignoreMap[n.Name] {
									br.skipped = append(br.skipped, n.Name)
								} else if err := o.stopContainer(timeoutCtx, n); err != nil {
									o.logger.Error("Failed to stop container",
										"container", n.Name,
										"component", compIndex,
										"batch", batchIndex,
										"error", err)
									br.failed = append(br.failed, n.Name)
								} else {
									br.stopped = append(br.stopped, n.Name)
								}

								batchChan <- br
							}
						}()
					}

					for _, node := range batch {
						jobChan <- node
					}
					close(jobChan)

					// Collect results from this batch
					for range batch {
						br := <-batchChan
						compResult.stopped = append(compResult.stopped, br.stopped...)
						compResult.skipped = append(compResult.skipped, br.skipped...)
						compResult.failed = append(compResult.failed, br.failed...)
					}
				}

				resultChan <- compResult
			}
		}()
	}

	for componentIdx, component := range components {
		workChan <- componentWork{idx: componentIdx, comp: component}
	}
	close(workChan)

	// Collect results from all components
	result := &StopResult{
		Stopped: []string{},
		Skipped: []string{},
		Failed:  []string{},
	}

	for range components {
		compResult := <-resultChan
		result.Stopped = append(result.Stopped, compResult.stopped...)
		result.Skipped = append(result.Skipped, compResult.skipped...)
		result.Failed = append(result.Failed, compResult.failed...)
	}

	o.logger.Info("Container shutdown complete",
		"stopped", len(result.Stopped),
		"skipped", len(result.Skipped),
		"failed", len(result.Failed))

	return result, nil
}

// startContainer starts a single container with health check and delay support
func (o *Orchestrator) startContainer(ctx context.Context, node *graph.Node) error {
	// Check if already running
	running, err := o.docker.IsContainerRunning(ctx, node.Name)
	if err != nil {
		return fmt.Errorf("failed to check container status: %w", err)
	}

	if running {
		o.logger.Debug("Container already running, skipping",
			"container", node.Name)
		return nil
	}

	o.logger.Info("Starting container",
		"container", node.Name,
		"delay", node.StartupDelay,
		"wait_healthcheck", node.WaitForHealthcheck)

	// Wait for parent dependencies' health checks if configured
	if node.WaitForHealthcheck && len(node.Parents) > 0 {
		o.logger.Info("Waiting for parent dependencies' health checks",
			"container", node.Name,
			"parent_count", len(node.Parents))

		for _, parent := range node.Parents {
			if parent.IsPlaceholder {
				continue
			}

			// Check if parent has a healthcheck
			hasHealthCheck, err := o.docker.HasHealthCheck(ctx, parent.Name)
			if err != nil {
				o.logger.Warn("Failed to check parent health config",
					"container", node.Name,
					"parent", parent.Name,
					"error", err)
				continue
			}

			if !hasHealthCheck {
				o.logger.Debug("Parent has no healthcheck, skipping",
					"container", node.Name,
					"parent", parent.Name)
				continue
			}

			// Wait for parent to be healthy
			if err := o.waitForHealthy(ctx, parent); err != nil {
				o.logger.Warn("Parent health check wait failed, continuing anyway",
					"container", node.Name,
					"parent", parent.Name,
					"error", err)
				// Don't fail - just warn and continue
			}
		}
	}

	// Apply startup delay if configured
	if node.StartupDelay > 0 {
		delay := time.Duration(node.StartupDelay) * time.Second
		o.logger.Debug("Applying startup delay",
			"container", node.Name,
			"delay", delay)

		select {
		case <-time.After(delay):
			// Delay completed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Start the container
	if err := o.docker.StartContainer(ctx, node.ID); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	o.logger.Info("Container started successfully",
		"container", node.Name)

	return nil
}

// stopContainer stops a single container using its configured StopTimeout
func (o *Orchestrator) stopContainer(ctx context.Context, node *graph.Node) error {
	// Check if already stopped
	running, err := o.docker.IsContainerRunning(ctx, node.Name)
	if err != nil {
		return fmt.Errorf("failed to check container status: %w", err)
	}

	if !running {
		o.logger.Debug("Container already stopped, skipping",
			"container", node.Name)
		return nil
	}

	// Use container's configured StopTimeout (or Docker's default of 10s if nil)
	var timeout int
	if node.StopTimeout != nil {
		timeout = *node.StopTimeout
		o.logger.Info("Stopping container",
			"container", node.Name,
			"timeout", timeout)
	} else {
		timeout = 10 // Docker default
		o.logger.Info("Stopping container",
			"container", node.Name,
			"timeout", "default (10s)")
	}

	// Stop the container
	if err := o.docker.StopContainer(ctx, node.ID, timeout); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	o.logger.Info("Container stopped successfully",
		"container", node.Name)

	return nil
}

// waitForHealthy waits for a container to become healthy
func (o *Orchestrator) waitForHealthy(ctx context.Context, node *graph.Node) error {
	// Check if container has health check configured
	hasHealthCheck, err := o.docker.HasHealthCheck(ctx, node.Name)
	if err != nil {
		return fmt.Errorf("failed to check health config: %w", err)
	}

	if !hasHealthCheck {
		o.logger.Warn("Health check expected but not configured",
			"container", node.Name)
		return nil // Don't fail, just continue
	}

	o.logger.Info("Waiting for container to become healthy",
		"container", node.Name)

	// Poll for healthy status
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(60 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			o.logger.Warn("Health check timeout, continuing anyway",
				"container", node.Name)
			return nil // Don't fail, just warn
		case <-ticker.C:
			status, err := o.docker.GetHealthStatus(ctx, node.Name)
			if err != nil {
				o.logger.Debug("Failed to get health status, retrying",
					"container", node.Name,
					"error", err)
				continue // Retry
			}

			o.logger.Debug("Health check status",
				"container", node.Name,
				"status", status)

			if status == "healthy" {
				o.logger.Info("Container is healthy",
					"container", node.Name)
				return nil
			}
		}
	}
}
