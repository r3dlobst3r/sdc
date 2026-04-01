package graph

import (
	"context"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/r3dlobst3r/sdc/pkg/logger"
)

// mockDockerClientBench is a mock implementation for benchmarks
type mockDockerClientBench struct{}

func (m *mockDockerClientBench) GetContainer(ctx context.Context, containerID string) (*client.ContainerInspectResult, error) {
	return &client.ContainerInspectResult{
		Container: container.InspectResponse{
			Config: &container.Config{
				StopTimeout: nil,
			},
		},
	}, nil
}

// BenchmarkBuildGraph benchmarks dependency graph building
func BenchmarkBuildGraph(b *testing.B) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClientBench{}
	builder := NewBuilder(mockDocker, log)

	// Create containers with various dependency configurations
	containers := []container.Summary{
		{
			Names:  []string{"/database"},
			Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"},
		},
		{
			Names: []string{"/cache"},
			Labels: map[string]string{
				"com.github.saltbox.saltbox_managed": "true",
				"com.github.saltbox.depends_on":      "database",
			},
		},
		{
			Names: []string{"/api"},
			Labels: map[string]string{
				"com.github.saltbox.saltbox_managed": "true",
				"com.github.saltbox.depends_on":      "database,cache",
			},
		},
		{
			Names: []string{"/worker"},
			Labels: map[string]string{
				"com.github.saltbox.saltbox_managed": "true",
				"com.github.saltbox.depends_on":      "database,cache",
			},
		},
		{
			Names: []string{"/frontend"},
			Labels: map[string]string{
				"com.github.saltbox.saltbox_managed": "true",
				"com.github.saltbox.depends_on":      "api",
			},
		},
	}

	for b.Loop() {
		_, err := builder.Build(context.Background(), containers)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildLargeGraph benchmarks graph building with many containers
func BenchmarkBuildLargeGraph(b *testing.B) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClientBench{}
	builder := NewBuilder(mockDocker, log)

	// Create 100 containers with dependencies
	containers := make([]container.Summary, 100)
	for i := range 100 {
		name := string(rune('a'+(i%26))) + string(rune('0'+(i/26)))
		cont := container.Summary{
			Names:  []string{"/" + name},
			Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"},
		}

		// Add dependency to previous container (linear chain)
		if i > 0 {
			prevName := string(rune('a'+((i-1)%26))) + string(rune('0'+((i-1)/26)))
			cont.Labels["com.github.saltbox.depends_on"] = prevName
		}

		containers[i] = cont
	}

	for b.Loop() {
		_, err := builder.Build(context.Background(), containers)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTopologicalSort benchmarks topological sorting
func BenchmarkTopologicalSort(b *testing.B) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClientBench{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
		{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
		{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
		{Names: []string{"/d"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b,c"}},
	}

	g, err := builder.Build(context.Background(), containers)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		_, err := g.GetStartupBatches()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCircularDependencyDetection benchmarks circular dependency detection
func BenchmarkCircularDependencyDetection(b *testing.B) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClientBench{}
	builder := NewBuilder(mockDocker, log)

	containers := []container.Summary{
		{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "c"}},
		{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
		{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b"}},
	}

	g, err := builder.Build(context.Background(), containers)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		_, _ = g.GetStartupBatches() // Will return error due to circular dependency
	}
}
