package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/r3dlobst3r/sdc/internal/jobs"
	"github.com/r3dlobst3r/sdc/pkg/logger"
)

// Server represents the API server
type Server struct {
	jobManager    *jobs.Manager
	logger        *logger.Logger
	isBlocked     bool
	blockMutex    sync.RWMutex
	unblockCancel context.CancelFunc
}

// NewServer creates a new API server
func NewServer(jobManager *jobs.Manager, logger *logger.Logger) *Server {
	return &Server{
		jobManager: jobManager,
		logger:     logger,
	}
}

// Router creates and configures the HTTP router
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	// Middleware stack
	r.Use(s.RecoveryMiddleware)
	r.Use(s.LoggingMiddleware)

	// Main API routes (spec-compliant)
	r.Post("/start", s.HandleStartContainers)
	r.Post("/stop", s.HandleStopContainers)
	r.Get("/ping", s.HandleHealth)

	// Block/unblock routes
	r.Post("/block", s.HandleBlock)
	r.Post("/block/{duration}", s.HandleBlock)
	r.Post("/unblock", s.HandleUnblock)

	// Job status route
	r.Get("/job_status/{job_id}", s.HandleGetJobStatus)

	return r
}

// JobResponse represents a job response
type JobResponse struct {
	JobID string `json:"job_id"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

type jobRequest struct {
	Timeout *int     `json:"timeout"`
	Ignore  []string `json:"ignore"`
}

func parseJobRequest(r *http.Request) (*jobRequest, error) {
	if r.Body == nil {
		return &jobRequest{}, nil
	}

	var req jobRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		if errors.Is(err, io.EOF) {
			return &jobRequest{}, nil
		}
		return nil, err
	}

	return &req, nil
}

// HandleStartContainers handles POST /start
func (s *Server) HandleStartContainers(w http.ResponseWriter, r *http.Request) {
	// Check if operations are blocked
	s.blockMutex.RLock()
	blocked := s.isBlocked
	s.blockMutex.RUnlock()

	if blocked {
		s.writeError(w, http.StatusServiceUnavailable, "Operation blocked")
		return
	}

	// Parse JSON body if provided
	timeout := 600 // 10 minutes default
	var ignore []string

	bodyReq, err := parseJobRequest(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if bodyReq.Timeout != nil {
		timeout = *bodyReq.Timeout
	}
	if len(bodyReq.Ignore) > 0 {
		ignore = append(ignore, bodyReq.Ignore...)
	}

	// Parse query parameters
	if timeoutStr := r.URL.Query().Get("timeout"); timeoutStr != "" {
		if parsedTimeout, err := strconv.Atoi(timeoutStr); err == nil {
			timeout = parsedTimeout
		}
	}

	// Create and submit job
	job := jobs.NewJob(jobs.JobTypeStart, timeout, ignore)
	if err := s.jobManager.Submit(job); err != nil {
		if errors.Is(err, jobs.ErrQueueFull) {
			s.writeError(w, http.StatusServiceUnavailable, "Job queue is full")
			return
		}
		s.logger.Error("Failed to submit job", "error", err)
		s.writeError(w, http.StatusInternalServerError, "Failed to submit job")
		return
	}

	s.logger.Info("Start job created",
		"job_id", job.ID,
		"timeout", timeout)

	s.writeJSON(w, http.StatusOK, JobResponse{
		JobID: job.ID,
	})
}

// HandleStopContainers handles POST /stop
func (s *Server) HandleStopContainers(w http.ResponseWriter, r *http.Request) {
	// Check if operations are blocked
	s.blockMutex.RLock()
	blocked := s.isBlocked
	s.blockMutex.RUnlock()

	if blocked {
		s.writeError(w, http.StatusServiceUnavailable, "Operation blocked")
		return
	}

	// Parse JSON body if provided
	timeout := 300 // 5 minutes default
	var ignore []string

	bodyReq, err := parseJobRequest(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if bodyReq.Timeout != nil {
		timeout = *bodyReq.Timeout
	}
	if len(bodyReq.Ignore) > 0 {
		ignore = append(ignore, bodyReq.Ignore...)
	}

	// Parse timeout query parameter
	if timeoutStr := r.URL.Query().Get("timeout"); timeoutStr != "" {
		if parsedTimeout, err := strconv.Atoi(timeoutStr); err == nil {
			timeout = parsedTimeout
		}
	}

	// Parse ignore query parameter (supports both comma-separated and repeated params)
	query := r.URL.Query()

	// Handle repeated params: ?ignore=traefik&ignore=nginx
	if ignoreParams := query["ignore"]; len(ignoreParams) > 0 {
		for _, param := range ignoreParams {
			// Also support comma-separated within each param: ?ignore=traefik,nginx
			parts := strings.SplitSeq(param, ",")
			for part := range parts {
				if trimmed := strings.TrimSpace(part); trimmed != "" {
					ignore = append(ignore, trimmed)
				}
			}
		}
	}

	// Create and submit job
	job := jobs.NewJob(jobs.JobTypeStop, timeout, ignore)
	if err := s.jobManager.Submit(job); err != nil {
		if errors.Is(err, jobs.ErrQueueFull) {
			s.writeError(w, http.StatusServiceUnavailable, "Job queue is full")
			return
		}
		s.logger.Error("Failed to submit job", "error", err)
		s.writeError(w, http.StatusInternalServerError, "Failed to submit job")
		return
	}

	s.logger.Info("Stop job created",
		"job_id", job.ID,
		"timeout", timeout,
		"ignore", ignore)

	s.writeJSON(w, http.StatusOK, JobResponse{
		JobID: job.ID,
	})
}

// HandleGetJobStatus handles GET /job_status/{job_id}
func (s *Server) HandleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "job_id")

	job, err := s.jobManager.Get(jobID)
	if err != nil {
		s.logger.Debug("Job not found", "job_id", jobID)
		s.writeJSON(w, http.StatusNotFound, map[string]string{
			"status": "not_found",
		})
		return
	}

	s.writeJSON(w, http.StatusOK, job)
}

// HandleHealth handles GET /ping
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// HandleBlock handles POST /block/{duration}
func (s *Server) HandleBlock(w http.ResponseWriter, r *http.Request) {
	// Parse duration from URL parameter (in minutes)
	durationStr := chi.URLParam(r, "duration")
	duration := 10 // Default 10 minutes
	if durationStr != "" {
		if parsedDuration, err := strconv.Atoi(durationStr); err == nil && parsedDuration > 0 {
			duration = parsedDuration
		}
	}

	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	// Cancel any existing unblock timer
	if s.unblockCancel != nil {
		s.unblockCancel()
	}

	// Set blocked state
	s.isBlocked = true

	// Create context for auto-unblock
	ctx, cancel := context.WithCancel(context.Background())
	s.unblockCancel = cancel

	// Start auto-unblock timer
	go func() {
		timer := time.NewTimer(time.Duration(duration) * time.Minute)
		defer timer.Stop()

		select {
		case <-timer.C:
			s.blockMutex.Lock()
			s.isBlocked = false
			s.unblockCancel = nil
			s.blockMutex.Unlock()
			s.logger.Info("Auto unblock complete")
		case <-ctx.Done():
			// Timer was cancelled
			return
		}
	}()

	s.logger.Info("Operations are now blocked", "duration_minutes", duration)
	s.writeJSON(w, http.StatusOK, map[string]string{
		"message": "Operations are now blocked for " + strconv.Itoa(duration) + " minutes",
	})
}

// HandleUnblock handles POST /unblock
func (s *Server) HandleUnblock(w http.ResponseWriter, r *http.Request) {
	s.blockMutex.Lock()
	defer s.blockMutex.Unlock()

	// Cancel auto-unblock timer if exists
	if s.unblockCancel != nil {
		s.unblockCancel()
		s.unblockCancel = nil
	}

	// Unblock operations
	s.isBlocked = false

	s.logger.Info("Operations are now unblocked")
	s.writeJSON(w, http.StatusOK, map[string]string{
		"message": "Operations are now unblocked",
	})
}

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("Failed to encode JSON response", "error", err)
	}
}

// writeError writes an error JSON response
func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, ErrorResponse{Error: message})
}
