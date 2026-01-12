package graph

import (
	"github.com/moby/moby/api/types/container"
)

// Node represents a container in the dependency graph
type Node struct {
	// Container information
	ID            string
	Name          string
	Labels        map[string]string
	IsRunning     bool
	IsPlaceholder bool // True if container doesn't exist but is referenced as a dependency

	// Dependency information
	Parents  []*Node // Containers this one depends on (must start first)
	Children []*Node // Containers that depend on this one (start after)

	// Startup configuration from labels
	StartupDelay       int  // Delay in seconds after dependencies are ready
	WaitForHealthcheck bool // Wait for health check to pass

	// Container configuration
	StopTimeout *int // Container's configured stop timeout in seconds (nil = Docker default of 10s)

	// Internal state for topological sort
	visited   bool
	inStack   bool
	sortIndex int
}

// Graph represents the complete dependency graph of containers
type Graph struct {
	Nodes map[string]*Node // Key is container name
}

// SortedContainers represents the result of topological sort
type SortedContainers struct {
	StartupOrder  []*Node // Order for starting containers
	ShutdownOrder []*Node // Order for stopping containers (reverse of startup)
}

// NewNode creates a new graph node from container information
func NewNode(summary container.Summary) *Node {
	name := containerName(summary)

	return &Node{
		ID:                 summary.ID,
		Name:               name,
		Labels:             summary.Labels,
		IsRunning:          summary.State == "running",
		IsPlaceholder:      false,
		Parents:            []*Node{},
		Children:           []*Node{},
		StartupDelay:       0,
		WaitForHealthcheck: false,
		visited:            false,
		inStack:            false,
		sortIndex:          -1,
	}
}

func containerName(summary container.Summary) string {
	for _, name := range summary.Names {
		if len(name) > 0 && name[0] == '/' {
			name = name[1:]
		}
		if name != "" {
			return name
		}
	}

	if summary.ID != "" {
		return summary.ID
	}

	return ""
}

// NewPlaceholderNode creates a placeholder node for a missing dependency
func NewPlaceholderNode(name string) *Node {
	return &Node{
		ID:                 "",
		Name:               name,
		Labels:             map[string]string{},
		IsRunning:          false,
		IsPlaceholder:      true,
		Parents:            []*Node{},
		Children:           []*Node{},
		StartupDelay:       0,
		WaitForHealthcheck: false,
		visited:            false,
		inStack:            false,
		sortIndex:          -1,
	}
}

// AddParent adds a parent dependency (this node depends on parent)
func (n *Node) AddParent(parent *Node) {
	n.Parents = append(n.Parents, parent)
	parent.Children = append(parent.Children, n)
}

// HasParents returns true if the node has any parent dependencies
func (n *Node) HasParents() bool {
	return len(n.Parents) > 0
}

// HasChildren returns true if the node has any child dependencies
func (n *Node) HasChildren() bool {
	return len(n.Children) > 0
}
