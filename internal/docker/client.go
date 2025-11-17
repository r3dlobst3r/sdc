package docker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/saltyorg/sdc/pkg/logger"
)

// Client wraps the Docker client with custom methods
type Client struct {
	cli    *client.Client
	logger *logger.Logger
}

// ConnectionConfig holds configuration for Docker connection retries
type ConnectionConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Timeout        time.Duration
}

// DefaultConnectionConfig returns the default connection configuration
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Timeout:        10 * time.Second,
	}
}

// New creates a new Docker client wrapper
func New(host string, logger *logger.Logger) (*Client, error) {
	return NewWithConfig(host, logger, DefaultConnectionConfig())
}

// NewWithConfig creates a new Docker client wrapper with custom connection config
func NewWithConfig(host string, logger *logger.Logger, cfg ConnectionConfig) (*Client, error) {
	var opts []client.Opt

	if host != "" {
		opts = append(opts, client.WithHost(host))
	}

	opts = append(opts, client.WithAPIVersionNegotiation())

	var cli *client.Client
	var err error
	backoff := cfg.InitialBackoff

	// Retry connection with exponential backoff
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		cli, err = client.New(opts...)
		if err != nil {
			if attempt < cfg.MaxRetries {
				logger.Warn("Failed to create Docker client, retrying",
					"attempt", attempt+1,
					"max_retries", cfg.MaxRetries,
					"backoff", backoff,
					"error", err)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
				continue
			}
			return nil, fmt.Errorf("failed to create docker client after %d attempts: %w", cfg.MaxRetries+1, err)
		}

		// Validate connection by pinging Docker daemon
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		pingResult, pingErr := cli.Ping(ctx, client.PingOptions{})
		cancel()

		if pingErr != nil {
			cli.Close()
			if attempt < cfg.MaxRetries {
				logger.Warn("Failed to ping Docker daemon, retrying",
					"attempt", attempt+1,
					"max_retries", cfg.MaxRetries,
					"backoff", backoff,
					"error", pingErr)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
				continue
			}
			return nil, fmt.Errorf("failed to connect to docker daemon after %d attempts: %w", cfg.MaxRetries+1, pingErr)
		}

		// Get Docker version information
		ctx, cancel = context.WithTimeout(context.Background(), cfg.Timeout)
		versionInfo, versionErr := cli.ServerVersion(ctx, client.ServerVersionOptions{})
		cancel()

		if versionErr != nil {
			cli.Close()
			if attempt < cfg.MaxRetries {
				logger.Warn("Failed to get Docker version, retrying",
					"attempt", attempt+1,
					"max_retries", cfg.MaxRetries,
					"backoff", backoff,
					"error", versionErr)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
				continue
			}
			return nil, fmt.Errorf("failed to get docker version after %d attempts: %w", cfg.MaxRetries+1, versionErr)
		}

		// Log successful connection with version info
		logger.Info("Successfully connected to Docker daemon",
			"docker_version", versionInfo.Version,
			"api_version", versionInfo.APIVersion,
			"os_type", pingResult.OSType,
			"arch", versionInfo.Arch,
			"experimental", pingResult.Experimental)

		return &Client{
			cli:    cli,
			logger: logger,
		}, nil
	}

	return nil, fmt.Errorf("failed to create docker client after %d attempts: %w", cfg.MaxRetries+1, err)
}

// Close closes the Docker client connection
func (c *Client) Close() error {
	return c.cli.Close()
}

// Ping checks if Docker daemon is accessible
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.cli.Ping(ctx, client.PingOptions{})
	return err
}

// ListManagedContainers returns all containers with saltbox_managed=true label
func (c *Client) ListManagedContainers(ctx context.Context) ([]container.Summary, error) {
	filters := make(client.Filters).Add("label", "com.github.saltbox.saltbox_managed=true")

	result, err := c.cli.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: filters,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	return result.Items, nil
}

// GetContainer returns detailed container information
func (c *Client) GetContainer(ctx context.Context, containerID string) (*client.ContainerInspectResult, error) {
	info, err := c.cli.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container %s: %w", containerID, err)
	}

	return &info, nil
}

// StartContainer starts a container by name or ID
func (c *Client) StartContainer(ctx context.Context, containerID string) error {
	_, err := c.cli.ContainerStart(ctx, containerID, client.ContainerStartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start container %s: %w", containerID, err)
	}

	c.logger.Debug("Container started", "container", containerID)
	return nil
}

// StopContainer stops a container by name or ID
func (c *Client) StopContainer(ctx context.Context, containerID string, timeout int) error {
	_, err := c.cli.ContainerStop(ctx, containerID, client.ContainerStopOptions{
		Timeout: &timeout,
	})
	if err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerID, err)
	}

	c.logger.Debug("Container stopped", "container", containerID)
	return nil
}

// HasHealthCheck checks if a container has a health check configured
func (c *Client) HasHealthCheck(ctx context.Context, containerNameOrID string) (bool, error) {
	info, err := c.GetContainer(ctx, containerNameOrID)
	if err != nil {
		return false, err
	}

	return info.Container.Config.Healthcheck != nil, nil
}

// GetHealthStatus returns the health status of a container
// Returns: "healthy", "unhealthy", "starting", "none"
func (c *Client) GetHealthStatus(ctx context.Context, containerNameOrID string) (string, error) {
	info, err := c.GetContainer(ctx, containerNameOrID)
	if err != nil {
		return "", err
	}

	if info.Container.State.Health == nil {
		return "none", nil
	}

	return string(info.Container.State.Health.Status), nil
}

// IsContainerRunning checks if a container is currently running
func (c *Client) IsContainerRunning(ctx context.Context, containerNameOrID string) (bool, error) {
	info, err := c.GetContainer(ctx, containerNameOrID)
	if err != nil {
		return false, err
	}

	return info.Container.State.Running, nil
}

// GetContainerLogs retrieves container logs
func (c *Client) GetContainerLogs(ctx context.Context, containerID string) (string, error) {
	result, err := c.cli.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       "100",
	})
	if err != nil {
		return "", fmt.Errorf("failed to get logs for container %s: %w", containerID, err)
	}
	defer result.Close()

	data, err := io.ReadAll(result)
	if err != nil {
		return "", fmt.Errorf("failed to read logs: %w", err)
	}

	return string(data), nil
}
