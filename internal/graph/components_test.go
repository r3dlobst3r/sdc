package graph

import (
	"context"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/r3dlobst3r/sdc/pkg/logger"
)

func TestGetConnectedComponents(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	tests := []struct {
		name       string
		containers []container.Summary
		wantCount  int
		wantSizes  []int // Expected sizes of each component
	}{
		{
			name: "single independent containers",
			containers: []container.Summary{
				{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
			},
			wantCount: 3,
			wantSizes: []int{1, 1, 1},
		},
		{
			name: "one dependency chain",
			containers: []container.Summary{
				{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
				{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b"}},
			},
			wantCount: 1,
			wantSizes: []int{3},
		},
		{
			name: "two independent chains",
			containers: []container.Summary{
				{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
				{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/d"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "c"}},
			},
			wantCount: 2,
			wantSizes: []int{2, 2},
		},
		{
			name: "mixed: chain and independent",
			containers: []container.Summary{
				{Names: []string{"/database"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/cache"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/api"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "database"}},
				{Names: []string{"/frontend"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "api"}},
			},
			wantCount: 2,
			wantSizes: []int{3, 1}, // database->api->frontend (3) and cache (1)
		},
		{
			name: "complex diamond dependency",
			containers: []container.Summary{
				{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
				{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
				{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
				{Names: []string{"/d"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b,c"}},
			},
			wantCount: 1,
			wantSizes: []int{4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := builder.Build(context.Background(), tt.containers)
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}

			components, err := g.GetConnectedComponents()
			if err != nil {
				t.Fatalf("GetConnectedComponents() error = %v", err)
			}

			if len(components) != tt.wantCount {
				t.Errorf("GetConnectedComponents() component count = %v, want %v", len(components), tt.wantCount)
			}

			// Check component sizes (total containers in each component)
			gotSizes := make([]int, len(components))
			for i, comp := range components {
				total := 0
				for _, batch := range comp.Batches {
					total += len(batch)
				}
				gotSizes[i] = total
			}

			// Sort both slices to compare (order of components doesn't matter)
			sortIntSlice(gotSizes)
			sortIntSlice(tt.wantSizes)

			if !equalIntSlices(gotSizes, tt.wantSizes) {
				t.Errorf("GetConnectedComponents() component sizes = %v, want %v", gotSizes, tt.wantSizes)
			}
		})
	}
}

func TestGetConnectedComponentsOrdering(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	// Build a simple dependency chain: a -> b -> c
	containers := []container.Summary{
		{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
		{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
		{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b"}},
	}

	g, err := builder.Build(context.Background(), containers)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	components, err := g.GetConnectedComponents()
	if err != nil {
		t.Fatalf("GetConnectedComponents() error = %v", err)
	}

	if len(components) != 1 {
		t.Fatalf("Expected 1 component, got %v", len(components))
	}

	comp := components[0]
	if len(comp.Batches) != 3 {
		t.Fatalf("Expected 3 batches (one per depth level), got %v", len(comp.Batches))
	}

	// Verify startup order: batch 0 has 'a', batch 1 has 'b', batch 2 has 'c'
	if len(comp.Batches[0]) != 1 || comp.Batches[0][0].Name != "a" {
		t.Errorf("Expected batch 0 to contain 'a', got %v", GetNodeNames(comp.Batches[0]))
	}
	if len(comp.Batches[1]) != 1 || comp.Batches[1][0].Name != "b" {
		t.Errorf("Expected batch 1 to contain 'b', got %v", GetNodeNames(comp.Batches[1]))
	}
	if len(comp.Batches[2]) != 1 || comp.Batches[2][0].Name != "c" {
		t.Errorf("Expected batch 2 to contain 'c', got %v", GetNodeNames(comp.Batches[2]))
	}
}

func TestGetConnectedComponentsForShutdown(t *testing.T) {
	log, _ := logger.New(true)
	mockDocker := &mockDockerClient{}
	builder := NewBuilder(mockDocker, log)

	// Build a simple dependency chain: a -> b -> c
	containers := []container.Summary{
		{Names: []string{"/a"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true"}},
		{Names: []string{"/b"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "a"}},
		{Names: []string{"/c"}, Labels: map[string]string{"com.github.saltbox.saltbox_managed": "true", "com.github.saltbox.depends_on": "b"}},
	}

	g, err := builder.Build(context.Background(), containers)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	components, err := g.GetConnectedComponentsForShutdown()
	if err != nil {
		t.Fatalf("GetConnectedComponentsForShutdown() error = %v", err)
	}

	if len(components) != 1 {
		t.Fatalf("Expected 1 component, got %v", len(components))
	}

	comp := components[0]
	if len(comp.Batches) != 3 {
		t.Fatalf("Expected 3 batches, got %v", len(comp.Batches))
	}

	// Verify shutdown order: batch 0 has 'c', batch 1 has 'b', batch 2 has 'a' (reverse of startup)
	if len(comp.Batches[0]) != 1 || comp.Batches[0][0].Name != "c" {
		t.Errorf("Expected batch 0 to contain 'c', got %v", GetNodeNames(comp.Batches[0]))
	}
	if len(comp.Batches[1]) != 1 || comp.Batches[1][0].Name != "b" {
		t.Errorf("Expected batch 1 to contain 'b', got %v", GetNodeNames(comp.Batches[1]))
	}
	if len(comp.Batches[2]) != 1 || comp.Batches[2][0].Name != "a" {
		t.Errorf("Expected batch 2 to contain 'a', got %v", GetNodeNames(comp.Batches[2]))
	}
}

// Helper functions
func sortIntSlice(s []int) {
	for i := range s {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
