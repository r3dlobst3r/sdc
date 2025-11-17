package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/saltyorg/sdc/internal/client"
	"github.com/saltyorg/sdc/internal/config"
	"github.com/saltyorg/sdc/pkg/logger"
	"github.com/spf13/cobra"
)

var helperConfig config.HelperConfig

var helperCmd = &cobra.Command{
	Use:   "helper",
	Short: "Run in helper mode (Docker lifecycle integration)",
	Long: `Starts containers when Docker daemon starts and stops them when Docker stops.
This mode is designed to run as a systemd service with dependency on docker.service`,
	RunE: runHelper,
}

func init() {
	helperCmd.Flags().StringVar(&helperConfig.ControllerURL, "controller-url", "http://127.0.0.1:3377", "Controller API URL")
	helperCmd.Flags().DurationVar(&helperConfig.StartupDelay, "startup-delay", 5*time.Second, "Initial delay before starting containers")
	helperCmd.Flags().IntVar(&helperConfig.Timeout, "timeout", 600, "Operation timeout in seconds")
	helperCmd.Flags().DurationVar(&helperConfig.PollInterval, "poll-interval", 5*time.Second, "Job status polling interval")
	rootCmd.AddCommand(helperCmd)
}

func runHelper(cmd *cobra.Command, args []string) error {
	// Initialize logger
	log, err := logger.New(false)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer log.Sync()

	log.Info("Starting Saltbox Docker Controller Helper",
		"version", Version,
		"controller_url", helperConfig.ControllerURL,
		"startup_delay", helperConfig.StartupDelay,
		"timeout", helperConfig.Timeout)

	ctx := context.Background()

	// Create client
	apiClient := client.NewClient(helperConfig.ControllerURL, log)
	apiClient.SetUserAgent("sdc/" + Version)

	// Wait for controller to become ready
	log.Info("Waiting for controller to become ready")
	if err := apiClient.WaitForServerReady(ctx, 60*time.Second); err != nil {
		return fmt.Errorf("controller not ready: %w", err)
	}

	// Wait for initial delay
	log.Info("Waiting for startup delay", "delay", helperConfig.StartupDelay)
	time.Sleep(helperConfig.StartupDelay)

	// Submit start job
	log.Info("Submitting container start job")
	startResp, err := apiClient.StartContainers(ctx, helperConfig.Timeout, nil)
	if err != nil {
		// Check if operations are blocked (503 error)
		if isOperationBlocked(err) {
			log.Info("Container start operation is currently blocked, skipping")
		} else {
			return fmt.Errorf("failed to submit start job: %w", err)
		}
	} else {
		log.Info("Start job submitted, waiting for completion",
			"job_id", startResp.ID)

		// Wait for job to complete
		startJob, err := apiClient.WaitForJob(ctx, startResp.ID, helperConfig.PollInterval)
		if err != nil {
			return fmt.Errorf("failed to wait for start job: %w", err)
		}

		if startJob.Status == "failed" {
			totalContainers := len(startJob.Started) + len(startJob.Skipped) + len(startJob.Failed)
			if len(startJob.Started) == 0 && len(startJob.Skipped) == 0 {
				log.Error("Start job failed - all containers failed to start",
					"error", startJob.Error,
					"failed", startJob.Failed)
			} else {
				log.Error("Start job failed - some containers failed to start",
					"error", startJob.Error,
					"started", startJob.Started,
					"skipped", startJob.Skipped,
					"failed", startJob.Failed,
					"total", totalContainers)
			}
		} else {
			log.Info("Containers started successfully",
				"started", startJob.Started,
				"skipped", startJob.Skipped,
				"failed", startJob.Failed)
		}
	}

	// Setup signal handler for graceful stop
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	log.Info("Helper running, waiting for shutdown signal...")
	sig := <-sigChan

	log.Info("Shutdown signal received, stopping containers",
		"signal", sig.String())

	// Submit stop job
	stopResp, err := apiClient.StopContainers(ctx, helperConfig.Timeout, nil)
	if err != nil {
		// Check if operations are blocked (503 error)
		if isOperationBlocked(err) {
			log.Info("Container stop operation is currently blocked, skipping")
		} else {
			log.Error("Failed to submit stop job", "error", err)
			return err
		}
	} else {
		log.Info("Stop job submitted, waiting for completion",
			"job_id", stopResp.ID)

		// Wait for stop job to complete
		stopJob, err := apiClient.WaitForJob(ctx, stopResp.ID, helperConfig.PollInterval)
		if err != nil {
			log.Error("Failed to wait for stop job", "error", err)
			return err
		}

		if stopJob.Status == "failed" {
			totalContainers := len(stopJob.Stopped) + len(stopJob.Skipped) + len(stopJob.Failed)
			if len(stopJob.Stopped) == 0 && len(stopJob.Skipped) == 0 {
				log.Error("Stop job failed - all containers failed to stop",
					"error", stopJob.Error,
					"failed", stopJob.Failed)
			} else {
				log.Error("Stop job failed - some containers failed to stop",
					"error", stopJob.Error,
					"stopped", stopJob.Stopped,
					"skipped", stopJob.Skipped,
					"failed", stopJob.Failed,
					"total", totalContainers)
			}
		} else {
			log.Info("Containers stopped successfully",
				"stopped", stopJob.Stopped,
				"skipped", stopJob.Skipped,
				"failed", stopJob.Failed)
		}
	}

	log.Info("Helper shutdown complete")
	return nil
}

// isOperationBlocked checks if the error is due to operations being blocked (503)
func isOperationBlocked(err error) bool {
	if err == nil {
		return false
	}
	// Check if error contains "503" status code
	return strings.Contains(err.Error(), "status 503") || strings.Contains(err.Error(), "Operation blocked")
}
