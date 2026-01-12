package graph

// ComponentBatches represents a single connected component with its batches
type ComponentBatches struct {
	Batches [][]*Node // Batches of nodes that can run in parallel within this component
}

// GetConnectedComponents identifies independent subgraphs in the dependency graph.
// Each component is a set of containers that are connected through dependencies
// (directly or indirectly). Containers in different components have no dependency
// relationships with each other and can be processed in parallel.
//
// Returns a slice of components, where each component contains batches of nodes.
// Nodes within the same batch have no dependencies on each other and can run in parallel.
func (g *Graph) GetConnectedComponents() ([]*ComponentBatches, error) {
	visited := make(map[*Node]bool)
	var components []*ComponentBatches

	// Find all components using DFS
	for _, node := range g.Nodes {
		if !node.IsPlaceholder && !visited[node] {
			component := g.findComponent(node, visited)
			if len(component) > 0 {
				// Get batches for this component
				batches, err := g.getBatchesForComponent(component)
				if err != nil {
					return nil, err
				}
				components = append(components, &ComponentBatches{Batches: batches})
			}
		}
	}

	return components, nil
}

// findComponent performs DFS to find all nodes connected to the starting node
func (g *Graph) findComponent(start *Node, visited map[*Node]bool) []*Node {
	var component []*Node
	var stack []*Node
	stack = append(stack, start)

	for len(stack) > 0 {
		// Pop from stack
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if visited[node] || node.IsPlaceholder {
			continue
		}

		visited[node] = true
		component = append(component, node)

		// Add all connected nodes (parents and children) to stack
		for _, parent := range node.Parents {
			if !parent.IsPlaceholder && !visited[parent] {
				stack = append(stack, parent)
			}
		}
		for _, child := range node.Children {
			if !child.IsPlaceholder && !visited[child] {
				stack = append(stack, child)
			}
		}
	}

	return component
}

// getBatchesForComponent returns batches of nodes for a component
// Nodes within the same batch have no dependencies on each other
func (g *Graph) getBatchesForComponent(component []*Node) ([][]*Node, error) {
	// Create a subgraph containing only this component
	subgraph := &Graph{
		Nodes: make(map[string]*Node),
	}

	for _, node := range component {
		subgraph.Nodes[node.Name] = node
	}

	// Use existing topological sort to get batches
	batches, err := subgraph.GetStartupBatches()
	if err != nil {
		return nil, err
	}

	return batches, nil
}

// GetConnectedComponentsForShutdown returns components ordered for shutdown
// (same components but batches are reversed)
func (g *Graph) GetConnectedComponentsForShutdown() ([]*ComponentBatches, error) {
	components, err := g.GetConnectedComponents()
	if err != nil {
		return nil, err
	}

	// Reverse batches within each component
	shutdownComponents := make([]*ComponentBatches, len(components))
	for i, comp := range components {
		reversedBatches := make([][]*Node, len(comp.Batches))
		for j, batch := range comp.Batches {
			reversedBatches[len(comp.Batches)-1-j] = batch
		}
		shutdownComponents[i] = &ComponentBatches{Batches: reversedBatches}
	}

	return shutdownComponents, nil
}
