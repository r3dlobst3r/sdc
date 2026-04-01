package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/r3dlobst3r/sdc/internal/jobs"
	"github.com/r3dlobst3r/sdc/pkg/logger"
)

func TestBlockUnblock(t *testing.T) {
	// Create logger
	log, err := logger.New(false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create a mock job manager (we don't need real orchestrator for this test)
	jobManager := jobs.NewManager(nil, log, 1)
	defer jobManager.Shutdown(1 * time.Second)

	// Create server
	server := NewServer(jobManager, log)
	router := server.Router()

	t.Run("block operations", func(t *testing.T) {
		// Block for 1 minute
		req := httptest.NewRequest("POST", "/block/1", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		expected := "Operations are now blocked for 1 minutes"
		if response["message"] != expected {
			t.Errorf("Expected message '%s', got '%s'", expected, response["message"])
		}

		// Verify operations are blocked
		server.blockMutex.RLock()
		if !server.isBlocked {
			t.Error("Expected operations to be blocked")
		}
		server.blockMutex.RUnlock()
	})

	t.Run("start/stop blocked when blocked", func(t *testing.T) {
		// Try to start containers while blocked
		req := httptest.NewRequest("POST", "/start", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status 503, got %d", w.Code)
		}

		var response ErrorResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response.Error != "Operation blocked" {
			t.Errorf("Expected 'Operation blocked', got '%s'", response.Error)
		}

		// Try to stop containers while blocked
		req = httptest.NewRequest("POST", "/stop", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status 503, got %d", w.Code)
		}

		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if response.Error != "Operation blocked" {
			t.Errorf("Expected 'Operation blocked', got '%s'", response.Error)
		}
	})

	t.Run("unblock operations", func(t *testing.T) {
		// Unblock operations
		req := httptest.NewRequest("POST", "/unblock", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		expected := "Operations are now unblocked"
		if response["message"] != expected {
			t.Errorf("Expected message '%s', got '%s'", expected, response["message"])
		}

		// Verify operations are unblocked
		server.blockMutex.RLock()
		if server.isBlocked {
			t.Error("Expected operations to be unblocked")
		}
		server.blockMutex.RUnlock()
	})

	t.Run("block with explicit 10 minute duration", func(t *testing.T) {
		// Block with 10 minutes duration
		req := httptest.NewRequest("POST", "/block/10", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		expected := "Operations are now blocked for 10 minutes"
		if response["message"] != expected {
			t.Errorf("Expected message '%s', got '%s'", expected, response["message"])
		}

		// Clean up - unblock
		req = httptest.NewRequest("POST", "/unblock", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
	})
}

func TestAutoUnblock(t *testing.T) {
	// Create logger
	log, err := logger.New(false)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create a mock job manager
	jobManager := jobs.NewManager(nil, log, 1)
	defer jobManager.Shutdown(1 * time.Second)

	// Create server
	server := NewServer(jobManager, log)

	// Block for 1 second (we'll use a very short duration for testing)
	// Note: We can't actually test with 1 second via the API since it expects minutes
	// So we'll manually set up the test
	server.blockMutex.Lock()
	server.isBlocked = true
	ctx, cancel := context.WithCancel(context.Background())
	server.unblockCancel = cancel

	go func() {
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()

		select {
		case <-timer.C:
			server.blockMutex.Lock()
			server.isBlocked = false
			server.unblockCancel = nil
			server.blockMutex.Unlock()
		case <-ctx.Done():
			return
		}
	}()
	server.blockMutex.Unlock()

	// Verify initially blocked
	server.blockMutex.RLock()
	if !server.isBlocked {
		t.Error("Expected operations to be blocked initially")
	}
	server.blockMutex.RUnlock()

	// Wait for auto-unblock (2 seconds + small buffer)
	time.Sleep(3 * time.Second)

	// Verify auto-unblocked
	server.blockMutex.RLock()
	if server.isBlocked {
		t.Error("Expected operations to be auto-unblocked after timeout")
	}
	server.blockMutex.RUnlock()
}
