package graph

import (
	"context"
	"fmt"
	"slices"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/r3dlobst3r/sdc/internal/docker"
	"github.com/r3dlobst3r/sdc/pkg/logger"
)

// Builder constructs dependency graphs from container information
type Builder struct {
	docker DockerClient
	logger *logger.Logger
}

// DockerClient interface for container operations needed by the builder
type DockerClient interface {
	GetContainer(ctx context.Context, containerID string) (*client.ContainerInspectResult, error)
}

// NewBuilder creates a new graph builder
func NewBuilder(dockerClient DockerClient, logger *logger.Logger) *Builder {
	return &Builder{
		docker: dockerClient,
		logger: logger,
	}
}

// Build creates a dependency graph from a list of containers
func (b *Builder) Build(ctx context.Context, containers []container.Summary) (*Graph, error) {
	graph := &Graph{
		Nodes: make(map[string]*Node),
	}

	// First pass: Create nodes for all containers
	for _, c := range containers {
		node := NewNode(c)

		// Parse labels to extract dependency information
		labels := docker.ParseLabels(c.Labels)

		// Skip if not managed or controller disabled
		if !labels.IsManaged() {
			b.logger.Debug("Skipping unmanaged container",
				"container", node.Name)
			continue
		}

		node.StartupDelay = labels.GetStartupDelay()
		node.WaitForHealthcheck = labels.ShouldWaitForHealthcheck()

		// Fetch container details to get StopTimeout
		inspectResult, err := b.docker.GetContainer(ctx, c.ID)
		if err != nil {
			b.logger.Warn("Failed to inspect container for timeout",
				"container", node.Name,
				"error", err)
		} else if inspectResult.Container.Config != nil {
			node.StopTimeout = inspectResult.Container.Config.StopTimeout
		}

		graph.Nodes[node.Name] = node

		b.logger.Debug("Added container to graph",
			"container", node.Name,
			"startup_delay", node.StartupDelay,
			"wait_healthcheck", node.WaitForHealthcheck,
			"stop_timeout", node.StopTimeout)
	}

	// Second pass: Build dependency relationships
	for _, c := range containers {
		name := containerName(c)
		if name == "" {
			b.logger.Warn("Skipping container with no name or ID")
			continue
		}

		node, exists := graph.Nodes[name]
		if !exists {
			continue // Skip unmanaged containers
		}

		labels := docker.ParseLabels(c.Labels)
		dependencies := labels.GetDependencies()

		for _, depName := range dependencies {
			parent, exists := graph.Nodes[depName]
			if !exists {
				// Create placeholder node for missing dependency
				b.logger.Warn("Dependency not found, creating placeholder",
					"container", node.Name,
					"dependency", depName)

				parent = NewPlaceholderNode(depName)
				graph.Nodes[depName] = parent
			}

			node.AddParent(parent)

			b.logger.Debug("Added dependency",
				"container", node.Name,
				"depends_on", parent.Name,
				"placeholder", parent.IsPlaceholder)
		}
	}

	b.logger.Info("Dependency graph built",
		"total_nodes", len(graph.Nodes),
		"managed_containers", b.countRealNodes(graph))

	return graph, nil
}

// countRealNodes counts non-placeholder nodes
func (b *Builder) countRealNodes(g *Graph) int {
	count := 0
	for _, node := range g.Nodes {
		if !node.IsPlaceholder {
			count++
		}
	}
	return count
}

// GetNode retrieves a node by container name
func (g *Graph) GetNode(name string) (*Node, bool) {
	node, exists := g.Nodes[name]
	return node, exists
}

// GetRootNodes returns all nodes with no parent dependencies
func (g *Graph) GetRootNodes() []*Node {
	var roots []*Node
	for _, node := range g.Nodes {
		if !node.HasParents() && !node.IsPlaceholder {
			roots = append(roots, node)
		}
	}
	return roots
}

// GetLeafNodes returns all nodes with no children
func (g *Graph) GetLeafNodes() []*Node {
	var leaves []*Node
	for _, node := range g.Nodes {
		if !node.HasChildren() && !node.IsPlaceholder {
			leaves = append(leaves, node)
		}
	}
	return leaves
}

// HasCycles checks if the graph contains circular dependencies
func (g *Graph) HasCycles() (bool, []string) {
	// Reset visited flags
	for _, node := range g.Nodes {
		node.visited = false
		node.inStack = false
	}

	var cycle []string

	// DFS to detect cycles
	var dfs func(*Node) bool
	dfs = func(node *Node) bool {
		if node.inStack {
			// Found a cycle
			cycle = append(cycle, node.Name)
			return true
		}

		if node.visited {
			return false
		}

		node.visited = true
		node.inStack = true

		if slices.ContainsFunc(node.Children, dfs) {
			cycle = append(cycle, node.Name)
			return true
		}

		node.inStack = false
		return false
	}

	for _, node := range g.Nodes {
		if !node.visited {
			if dfs(node) {
				// Reverse cycle to show proper order
				for i := 0; i < len(cycle)/2; i++ {
					cycle[i], cycle[len(cycle)-1-i] = cycle[len(cycle)-1-i], cycle[i]
				}
				return true, cycle
			}
		}
	}

	return false, nil
}

// Validate checks the graph for errors (cycles, invalid dependencies, etc.)
func (g *Graph) Validate() error {
	// Check for circular dependencies
	hasCycle, cycle := g.HasCycles()
	if hasCycle {
		return fmt.Errorf("circular dependency detected: %v", cycle)
	}

	// Count placeholder nodes
	placeholders := 0
	for _, node := range g.Nodes {
		if node.IsPlaceholder {
			placeholders++
		}
	}

	if placeholders > 0 {
		// This is a warning, not an error - we allow missing dependencies
		return nil
	}

	return nil
}
