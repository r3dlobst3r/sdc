# Saltbox Docker Controller (SDC)

A dependency-aware Docker container orchestrator for Saltbox, written in Go.

## Overview

Saltbox Docker Controller manages Docker container startup and shutdown based on dependency labels. It ensures containers start in the correct order, waits for health checks, and handles graceful shutdown in reverse dependency order.

**Features:**
- Dependency-aware orchestration using Docker labels
- Topological sort for optimal startup/shutdown order
- Parallel execution of independent containers
- Health check polling (60 second timeout per container)
- Job-based API for async operations
- Block/unblock operations for maintenance windows
- REST API server mode
- Helper mode for Docker daemon lifecycle integration
- Graceful shutdown handling
- Comprehensive test suite (80 tests)

## Building

Build using the Makefile:

```bash
make build
```

The binary will be created at `build/sdc`.

For all available targets:
```bash
make help
```

## Testing

Run all tests:
```bash
make test
```

Run tests with coverage:
```bash
make test-coverage
```

## Usage

### Server Mode
Start the REST API server:
```bash
./build/sdc server --host 127.0.0.1 --port 3377
```

Or use the Makefile:
```bash
make run-server
```

### Helper Mode
Run the helper daemon for automatic lifecycle management:
```bash
./build/sdc helper --controller-url http://127.0.0.1:3377
```

Or use the Makefile:
```bash
make run-helper
```

### Version Information
```bash
./build/sdc --version
```

## Architecture

### Package Structure

```
sdc/
├── cmd/controller/         # Main entry point (server/helper commands)
├── internal/
│   ├── api/               # HTTP handlers, middleware, and router
│   ├── client/            # HTTP client for helper mode
│   ├── config/            # Configuration management
│   ├── docker/            # Docker client wrapper and label parsing
│   ├── graph/             # Dependency graph and topological sort
│   ├── jobs/              # Job manager with worker pool
│   └── orchestrator/      # Container orchestration engine
└── pkg/logger/            # Structured logging (Zap)
```

### How It Works

1. **Label-based dependencies**: Containers declare dependencies via `com.github.saltbox.depends_on` labels
2. **Graph building**: Saltbox Docker Controller builds a dependency graph from all running containers
3. **Topological sort**: Determines optimal startup/shutdown order
4. **Batch execution**: Independent containers in each batch start/stop in parallel
5. **Health checking**: Polls container health status before proceeding to dependents
6. **Job tracking**: All operations are tracked as jobs with UUID and status

## Docker Labels

Saltbox Docker Controller uses Saltbox Docker labels to define container management and dependencies:

```yaml
labels:
  com.github.saltbox.saltbox_managed: "true"              # Required: Enable SDC management
  com.github.saltbox.saltbox_controller: "true"           # Optional: Enable/disable controller (default: true)
  com.github.saltbox.depends_on: "postgres,redis"         # Optional: Comma-separated dependencies
  com.github.saltbox.depends_on.delay: "5"                # Optional: Startup delay in seconds
  com.github.saltbox.depends_on.healthchecks: "true"      # Optional: Wait for healthchecks (default: false)
```

**Example docker-compose.yml:**
```yaml
services:
  postgres:
    image: postgres:15
    labels:
      com.github.saltbox.saltbox_managed: "true"
      com.github.saltbox.depends_on.delay: "10"

  redis:
    image: redis:7
    labels:
      com.github.saltbox.saltbox_managed: "true"
      com.github.saltbox.depends_on.delay: "5"

  app:
    image: myapp:latest
    labels:
      com.github.saltbox.saltbox_managed: "true"
      com.github.saltbox.depends_on: "postgres,redis"
      com.github.saltbox.depends_on.delay: "2"
      com.github.saltbox.depends_on.healthchecks: "true"
```

Saltbox Docker Controller will ensure `postgres` and `redis` start first (in parallel), wait for health checks and startup delays, then start `app`.

## API Endpoints

### Container Operations
- `POST /start` - Start containers in dependency order
  - Query params: `?timeout=600` (optional, default: 600)
  - Response: `{"job_id": "uuid"}`
  - Returns HTTP 503 if operations are blocked
- `POST /stop` - Stop containers in reverse dependency order
  - Query params: `?timeout=300&ignore=container1&ignore=container2` (optional, default timeout: 300)
  - Response: `{"job_id": "uuid"}`
  - Returns HTTP 503 if operations are blocked

### Block/Unblock Operations
- `POST /block/{duration}` - Block start/stop operations temporarily
  - `duration` parameter in minutes (default: 10)
  - Response: `{"message": "Operations are now blocked for N minutes"}`
  - Auto-unblocks after the specified duration
- `POST /unblock` - Manually unblock operations
  - Response: `{"message": "Operations are now unblocked"}`

### Job Status
- `GET /job_status/{job_id}` - Get job details and status
  - Response: Full job object with status, results, and timing information
  - Returns `{"status": "not_found"}` with HTTP 404 if job doesn't exist

### Health Check
- `GET /ping` - Health check endpoint
  - Response: `{"status": "healthy"}`

## Helper Mode Details

The helper mode is designed to run as a systemd service for automatic container lifecycle management:

**Lifecycle:**
1. Wait for controller server to become ready (60 second timeout)
2. Apply configured startup delay (default: 5 seconds)
3. Submit start job for all managed containers
4. Wait for job completion
5. Run until receiving SIGTERM/SIGINT
6. Submit stop job for all containers
7. Wait for stop completion and exit gracefully

**Blocked Operations Handling:**
When start/stop operations are blocked (HTTP 503 response), the helper will:
- Log an INFO message: "Container start/stop operation is currently blocked, skipping"
- Continue running without failing
- This allows the helper to gracefully handle maintenance windows

**Options:**
```bash
./build/sdc helper \
  --controller-url http://127.0.0.1:3377 \  # Controller API URL (default: http://127.0.0.1:3377)
  --startup-delay 5s \                       # Delay before starting containers (default: 5s)
  --timeout 600 \                            # Job timeout in seconds (default: 600)
  --poll-interval 5s                         # Status polling interval (default: 5s)
```

## License

GNU General Public License v3.0
