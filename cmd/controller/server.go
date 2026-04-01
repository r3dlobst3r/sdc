package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/r3dlobst3r/sdc/internal/api"
	"github.com/r3dlobst3r/sdc/internal/config"
	"github.com/r3dlobst3r/sdc/internal/docker"
	"github.com/r3dlobst3r/sdc/internal/jobs"
	"github.com/r3dlobst3r/sdc/internal/orchestrator"
	"github.com/r3dlobst3r/sdc/pkg/logger"
	"github.com/spf13/cobra"
)

var serverConfig config.ServerConfig

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the Docker controller API server",
	Long:  `Starts the REST API server that manages Docker container orchestration`,
	RunE:  runServer,
}

func init() {
	serverCmd.Flags().StringVar(&serverConfig.Host, "host", "127.0.0.1", "API server host")
	serverCmd.Flags().IntVar(&serverConfig.Port, "port", 3377, "API server port")
	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, args []string) error {
	// Initialize logger
	log, err := logger.New(false)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer log.Sync()

	log.Info("Starting Saltbox Docker Controller",
		"version", Version,
		"commit", GitCommit,
		"build_time", BuildTime,
		"host", serverConfig.Host,
		"port", serverConfig.Port,
	)

	// Initialize Docker client
	dockerClient, err := docker.New("", log)
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %w", err)
	}

	log.Info("Docker client initialized")

	// Initialize orchestrator
	orch := orchestrator.New(dockerClient, log)
	log.Info("Orchestrator initialized")

	// Initialize job manager with 3 workers
	jobManager := jobs.NewManager(orch, log, 3)

	// Initialize API server
	apiServer := api.NewServer(jobManager, log)
	router := apiServer.Router()

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in goroutine
	serverErrors := make(chan error, 1)
	go func() {
		log.Info("API server listening", "addr", addr)
		serverErrors <- srv.ListenAndServe()
	}()

	// Setup signal handling for graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// Block until we receive a signal or server error
	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)
	case sig := <-shutdown:
		log.Info("Shutdown signal received", "signal", sig.String())

		// Give outstanding requests 30 seconds to complete
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Shutdown HTTP server
		if err := srv.Shutdown(ctx); err != nil {
			srv.Close()
			log.Error("HTTP server shutdown error", "error", err)
		} else {
			log.Info("HTTP server stopped gracefully")
		}

		// Shutdown job manager
		if err := jobManager.Shutdown(10 * time.Second); err != nil {
			log.Error("Job manager shutdown error", "error", err)
		} else {
			log.Info("Job manager stopped gracefully")
		}

		log.Info("Server shutdown complete")
	}

	return nil
}
