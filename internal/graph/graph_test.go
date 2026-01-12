package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/saltyorg/sdc/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDockerClient is a mock implementation for testing
type mockDockerClient struct{}

func (m *mockDockerClient) GetContainer(ctx context.Context, containerID string) (*client.ContainerInspectResult, error) {
	// Return a mock result with no StopTimeout set
	return &client.ContainerInspectResult{
		Container: container.InspectResponse{
			Config: &container.Config{
				StopTimeout: nil,
			},
		},
	}, nil
}

// Helper function to create test containers
func createTestContainer(name string, managed bool, dependencies []string, delay int, healthcheck bool) container.Summary {
	labels := map[string]string{}

	if managed {
		labels["com.github.saltbox.saltbox_managed"] = "true"
	} else {
		labels["com.github.saltbox.saltbox_managed"] = "false"
	}

	if len(dependencies) > 0 {
		var depStr strings.Builder
		for i, dep := range dependencies {
			if i > 0 {
				depStr.WriteString(",")
			}
			depStr.WriteString(dep)
		}
		labels["com.github.saltbox.depends_on"] = depStr.String()
	}

	if delay > 0 {
		labels["com.github.saltbox.depends_on.delay"] = string(rune(delay + '0'))
	}

	if healthcheck {
		labels["com.github.saltbox.depends_on.healthchecks"] = "true"
	}

	return container.Summary{
		ID:     name + "-id",
		Names:  []string{"/" + name},
		Labels: labels,
		State:  "exited",
	}
}

func TestNewNode(t *testing.T) {
	c := createTestContainer("test", true, nil, 0, false)
	node := NewNode(c)

	assert.Equal(t, "test", node.Name)
	assert.Equal(t, "test-id", node.ID)
	assert.False(t, node.IsPlaceholder)
	assert.False(t, node.IsRunning)
	assert.Empty(t, node.Parents)
	assert.Empty(t, node.Children)
}

func TestNewPlaceholderNode(t *testing.T) {
	node := NewPlaceholderNode("missing")

	assert.Equal(t, "missing", node.Name)
	assert.True(t, node.IsPlaceholder)
	assert.Empty(t, node.ID)
}

func TestNode_AddParent(t *testing.T) {
	parent := NewPlaceholderNode("parent")
	child := NewPlaceholderNode("child")

	child.AddParent(parent)

	assert.Len(t, child.Parents, 1)
	assert.Equal(t, parent, child.Parents[0])
	assert.Len(t, parent.Children, 1)
	assert.Equal(t, child, parent.Children[0])
}

func TestBuilder_Build_SimpleGraph(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("nginx", true, nil, 0, false),
		createTestContainer("app", true, []string{"nginx"}, 5, true),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)
	require.NotNil(t, graph)

	assert.Len(t, graph.Nodes, 2)

	nginx, exists := graph.GetNode("nginx")
	require.True(t, exists)
	assert.Empty(t, nginx.Parents)
	assert.Len(t, nginx.Children, 1)

	app, exists := graph.GetNode("app")
	require.True(t, exists)
	assert.Len(t, app.Parents, 1)
	assert.Equal(t, nginx, app.Parents[0])
	assert.Equal(t, 5, app.StartupDelay)
	assert.True(t, app.WaitForHealthcheck)
}

func TestBuilder_Build_MissingDependency(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("app", true, []string{"redis"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)
	require.NotNil(t, graph)

	assert.Len(t, graph.Nodes, 2) // app + placeholder for redis

	app, exists := graph.GetNode("app")
	require.True(t, exists)

	redis, exists := graph.GetNode("redis")
	require.True(t, exists)
	assert.True(t, redis.IsPlaceholder)
	assert.Len(t, redis.Children, 1)
	assert.Equal(t, app, redis.Children[0])
}

func TestBuilder_Build_SkipUnmanaged(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("managed", true, nil, 0, false),
		createTestContainer("unmanaged", false, nil, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	assert.Len(t, graph.Nodes, 1)
	_, exists := graph.GetNode("managed")
	assert.True(t, exists)
	_, exists = graph.GetNode("unmanaged")
	assert.False(t, exists)
}

func TestGraph_GetRootNodes(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("root1", true, nil, 0, false),
		createTestContainer("root2", true, nil, 0, false),
		createTestContainer("child", true, []string{"root1"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	roots := graph.GetRootNodes()
	assert.Len(t, roots, 2)

	rootNames := []string{roots[0].Name, roots[1].Name}
	assert.Contains(t, rootNames, "root1")
	assert.Contains(t, rootNames, "root2")
}

func TestGraph_GetLeafNodes(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("parent", true, nil, 0, false),
		createTestContainer("child1", true, []string{"parent"}, 0, false),
		createTestContainer("child2", true, []string{"parent"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	leaves := graph.GetLeafNodes()
	assert.Len(t, leaves, 2)

	leafNames := []string{leaves[0].Name, leaves[1].Name}
	assert.Contains(t, leafNames, "child1")
	assert.Contains(t, leafNames, "child2")
}

func TestGraph_HasCycles_NoCycle(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("a", true, nil, 0, false),
		createTestContainer("b", true, []string{"a"}, 0, false),
		createTestContainer("c", true, []string{"b"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	hasCycle, cycle := graph.HasCycles()
	assert.False(t, hasCycle)
	assert.Nil(t, cycle)
}

func TestGraph_HasCycles_WithCycle(t *testing.T) {
	// Manually create a graph with a cycle since we can't create it via labels
	graph := &Graph{
		Nodes: make(map[string]*Node),
	}

	a := NewPlaceholderNode("a")
	b := NewPlaceholderNode("b")
	c := NewPlaceholderNode("c")

	graph.Nodes["a"] = a
	graph.Nodes["b"] = b
	graph.Nodes["c"] = c

	// Create cycle: a -> b -> c -> a
	b.AddParent(a)
	c.AddParent(b)
	a.AddParent(c)

	hasCycle, cycle := graph.HasCycles()
	assert.True(t, hasCycle)
	assert.NotNil(t, cycle)
	assert.Contains(t, cycle, "a")
	assert.Contains(t, cycle, "b")
	assert.Contains(t, cycle, "c")
}

func TestGraph_TopologicalSort_Linear(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		createTestContainer("a", true, nil, 0, false),
		createTestContainer("b", true, []string{"a"}, 0, false),
		createTestContainer("c", true, []string{"b"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.NotNil(t, sorted)

	assert.Len(t, sorted.StartupOrder, 3)
	assert.Equal(t, "a", sorted.StartupOrder[0].Name)
	assert.Equal(t, "b", sorted.StartupOrder[1].Name)
	assert.Equal(t, "c", sorted.StartupOrder[2].Name)

	// Shutdown should be reverse
	assert.Len(t, sorted.ShutdownOrder, 3)
	assert.Equal(t, "c", sorted.ShutdownOrder[0].Name)
	assert.Equal(t, "b", sorted.ShutdownOrder[1].Name)
	assert.Equal(t, "a", sorted.ShutdownOrder[2].Name)
}

func TestGraph_TopologicalSort_Diamond(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	// Diamond dependency:
	//     a
	//    / \
	//   b   c
	//    \ /
	//     d
	containers := []container.Summary{
		createTestContainer("a", true, nil, 0, false),
		createTestContainer("b", true, []string{"a"}, 0, false),
		createTestContainer("c", true, []string{"a"}, 0, false),
		createTestContainer("d", true, []string{"b", "c"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	sorted, err := graph.TopologicalSort()
	require.NoError(t, err)
	require.NotNil(t, sorted)

	assert.Len(t, sorted.StartupOrder, 4)

	// Verify 'a' comes before 'b' and 'c'
	aIndex := -1
	bIndex := -1
	cIndex := -1
	dIndex := -1

	for i, node := range sorted.StartupOrder {
		switch node.Name {
		case "a":
			aIndex = i
		case "b":
			bIndex = i
		case "c":
			cIndex = i
		case "d":
			dIndex = i
		}
	}

	assert.True(t, aIndex < bIndex, "a should come before b")
	assert.True(t, aIndex < cIndex, "a should come before c")
	assert.True(t, bIndex < dIndex, "b should come before d")
	assert.True(t, cIndex < dIndex, "c should come before d")
}

func TestGraph_GetStartupBatches(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	// Graph:
	//     a
	//    / \
	//   b   c
	//    \ /
	//     d
	containers := []container.Summary{
		createTestContainer("a", true, nil, 0, false),
		createTestContainer("b", true, []string{"a"}, 0, false),
		createTestContainer("c", true, []string{"a"}, 0, false),
		createTestContainer("d", true, []string{"b", "c"}, 0, false),
	}

	graph, err := builder.Build(context.Background(), containers)
	require.NoError(t, err)

	batches, err := graph.GetStartupBatches()
	require.NoError(t, err)

	assert.Len(t, batches, 3)

	// Batch 0: a (root)
	assert.Len(t, batches[0], 1)
	assert.Equal(t, "a", batches[0][0].Name)

	// Batch 1: b and c (can start in parallel)
	assert.Len(t, batches[1], 2)
	names := []string{batches[1][0].Name, batches[1][1].Name}
	assert.Contains(t, names, "b")
	assert.Contains(t, names, "c")

	// Batch 2: d (depends on both b and c)
	assert.Len(t, batches[2], 1)
	assert.Equal(t, "d", batches[2][0].Name)
}

func TestFilterByState(t *testing.T) {
	running := &Node{Name: "running", IsRunning: true}
	stopped := &Node{Name: "stopped", IsRunning: false}

	nodes := []*Node{running, stopped}

	runningNodes := FilterByState(nodes, true)
	assert.Len(t, runningNodes, 1)
	assert.Equal(t, "running", runningNodes[0].Name)

	stoppedNodes := FilterByState(nodes, false)
	assert.Len(t, stoppedNodes, 1)
	assert.Equal(t, "stopped", stoppedNodes[0].Name)
}

func TestFilterByNames(t *testing.T) {
	a := &Node{Name: "a"}
	b := &Node{Name: "b"}
	c := &Node{Name: "c"}

	nodes := []*Node{a, b, c}

	exclude := map[string]bool{"b": true}
	filtered := FilterByNames(nodes, exclude)

	assert.Len(t, filtered, 2)
	names := []string{filtered[0].Name, filtered[1].Name}
	assert.Contains(t, names, "a")
	assert.Contains(t, names, "c")
	assert.NotContains(t, names, "b")
}

func TestGetNodeNames(t *testing.T) {
	nodes := []*Node{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}

	names := GetNodeNames(nodes)
	assert.Equal(t, []string{"a", "b", "c"}, names)
}
