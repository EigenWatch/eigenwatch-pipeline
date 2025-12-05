# EigenWatch Pipeline

A unified Dagster-based data pipeline for processing EigenLayer protocol events and generating operator analytics. Combines event extraction from The Graph subgraph with comprehensive risk analysis and profiling.

## Architecture

The pipeline operates in distinct phases with clear separation of concerns:

```
Event Extraction → State Reconstruction → Daily Snapshots → Analytics
```

**Key Design Principles:**

- Entity-centric processing (timestamp-based cursors, no race conditions)
- Full state reconstruction from complete event history
- SQL-first approach for performance
- Idempotent operations (safe to retry)

## Project Structure

```
pipeline/
├── src/
│   ├── pipeline/              # Analytics & state management
│   │   ├── defs/             # Dagster assets & resources
│   │   ├── db/               # Database models
│   │   └── services/         # Processing & reconstruction logic
│   └── subgraph_pipeline/    # Event extraction
│       ├── defs/             # Event extraction assets
│       ├── models/           # Event & entity models
│       └── utils/            # Subgraph client
├── alembic/
│   ├── events/              # Event DB migrations
│   └── analytics/           # Analytics DB migrations
├── dagster.yaml             # Dagster instance config
└── .env                     # Environment variables
```

## Quick Start

### Prerequisites

- Docker and Docker Compose (recommended)
- OR Python 3.9-3.13 + PostgreSQL 12+ (for local development)
- The Graph API access

### Option 1: Docker Setup (Recommended)

The easiest way to run the pipeline is using Docker Compose, which handles all dependencies and database setup.

#### 1. Clone and Configure

```bash
# Clone the repository
git clone https://github.com/EigenWatch/eigenwatch-pipeline.git
cd eigenwatch-pipeline

# Copy the environment template
cp example.env .env

# Edit .env with your credentials
nano .env  # or use your preferred editor
```

**Required variables in `.env`:**
- `SUBGRAPH_ENDPOINT` - Your Graph API endpoint
- `SUBGRAPH_API_KEY` - Your Graph API key
- `POSTGRES_PASSWORD` - Database password (default: dagster123)

#### 2. Start Services

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f
```

This starts three services:
- **PostgreSQL** (port 5432) - Database with auto-initialized schemas
- **Dagster Webserver** (port 3001) - UI and API
- **Dagster Daemon** - Background scheduler

#### 3. Run Database Migrations

```bash
# Access the webserver container
docker exec -it eigenwatch-webserver bash

# Run migrations for both databases
cd alembic/analytics && alembic upgrade head
cd ../events/migrations && alembic upgrade head

exit
```

#### 4. Access Dagster UI

Open your browser: **http://localhost:3001**

#### 5. Stop Services

```bash
# Stop services (preserves data)
docker-compose down

# Stop and remove all data
docker-compose down -v
```

### Option 2: Local Development Setup

For development without Docker:

**Using uv (recommended):**

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate  # macOS/Linux

# Set up PostgreSQL databases
psql -U postgres -c "CREATE DATABASE dagster;"
psql -U postgres -c "CREATE DATABASE eigenwatch_staging_db;"
psql -U postgres -c "CREATE DATABASE eigenwatch_analytics;"

# Run migrations
make events-upgrade
make analytics-upgrade

# Start Dagster
dagster dev
```

**Using pip:**

```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -e ".[dev]"

# Then set up databases and run migrations as above
```

Visit http://localhost:3000 to access the Dagster UI.

## Environment Variables

Required variables in `.env`:

```bash
# Dagster Configuration
DAGSTER_HOME=/home/didi/Code/work/eigenwatch/pipeline/
DAGSTER_PG_URL=postgresql://postgres:secret@localhost:5432/dagster

# Analytics Pipeline - Database URLs
EVENTS_DB_URL=postgresql+psycopg2://postgres:secret@localhost:5432/eigenwatch_staging_db
ANALYTICS_DB_URL=postgresql+psycopg2://postgres:secret@localhost:5432/eigenwatch_analytics

# Subgraph Config
SUBGRAPH_ENDPOINT=https://api.studio.thegraph.com/query/YOUR_ID/YOUR_SUBGRAPH/version/latest
SUBGRAPH_API_KEY=your_subgraph_api_key_here
RESULTS_PER_QUERY=100

# Analytics Pipeline Configuration
DEFAULT_REGISTERED_AT=2025-05-31T03:22:47Z
SAFETY_BUFFER_BLOCKS=10
SAFETY_BUFFER_SECONDS=60
MAX_OPERATORS_PER_BATCH=100
MAX_BLOCKS_PER_RUN=1000
SNAPSHOT_HOUR_UTC=0

# Logging configuration
LOG_BATCH_PROGRESS_EVERY=10
ENABLE_DETAILED_LOGGING=true

# Analytics settings
VOLATILITY_WINDOWS=7,30,90
MIN_DATA_POINTS_FOR_ANALYTICS=7

# Performance optimization
USE_BULK_OPERATIONS=true
COMMIT_BATCH_SIZE=50

# Pagination limits
MAX_SKIP=5000
BATCH_SIZE=1000

# Retry settings
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5

# Development/Debug Settings
DEBUG=true
LOG_LEVEL=DEBUG
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1
```

See `example.env` for complete configuration options.

## Pipeline Schedules

The pipeline consists of two main components with different scheduling strategies:

### Event Extraction Pipeline (Subgraph)

Processes blockchain events from EigenLayer smart contracts via The Graph subgraph. Runs **4 times daily** (every 6 hours) with staggered execution to prevent overlap:

| Job                            | Contracts          | Schedule (UTC)             | Description                          |
| ------------------------------ | ------------------ | -------------------------- | ------------------------------------ |
| **delegation_manager_events**  | DelegationManager  | 00:00, 06:00, 12:00, 18:00 | Delegation and undelegation events   |
| **allocation_manager_events**  | AllocationManager  | 00:30, 06:30, 12:30, 18:30 | Allocation modifications and delays  |
| **avs_directory_events**       | AVSDirectory       | 01:00, 07:00, 13:00, 19:00 | AVS registration and operator status |
| **rewards_coordinator_events** | RewardsCoordinator | 01:30, 07:30, 13:30, 19:30 | Reward submissions and claims        |
| **strategy_manager_events**    | StrategyManager    | 02:00, 08:00, 14:00, 20:00 | Deposit and withdrawal events        |
| **eigenpod_manager_events**    | EigenPodManager    | 02:30, 08:30, 14:30, 20:30 | Pod shares and validator events      |

**Total extraction cycle:** ~3 hours (30 min intervals × 6 jobs)  
**Frequency:** Every 6 hours  
**Processing strategy:** Incremental with cursor-based checkpoints

### Analytics Pipeline (State & Metrics)

Processes extracted events into operator profiles and analytics. Runs on a coordinated schedule:

| Job                       | Schedule (UTC)                  | Frequency | Description                                                                                                                        |
| ------------------------- | ------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **operator_state_update** | Every 6 hours<br/>`0 */6 * * *` | 4x daily  | Extract changed operators from events and rebuild their complete state (TVL, delegators, AVS relationships, commissions, slashing) |
| **operator_snapshots**    | Daily at 00:05<br/>`5 0 * * *`  | Daily     | Create point-in-time snapshots of all operator data for historical analysis and trend tracking                                     |
| **operator_analytics**    | Daily at 00:30<br/>`30 0 * * *` | Daily     | Calculate risk scores, volatility metrics (7/30/90-day), concentration indices (HHI, Gini), and performance rankings               |

**State update duration:** ~5-30 minutes (varies with event volume)  
**Snapshot duration:** ~2-5 minutes  
**Analytics duration:** ~5-15 minutes  
**Processing strategy:** Full state reconstruction from complete event history (idempotent)

### Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│  CONTINUOUS (Every 6 Hours)                                 │
│  ┌────────────────┐    ┌─────────────────┐                 │
│  │ Event Jobs     │───▶│ State Rebuild   │                 │
│  │ (Staggered)    │    │ (Changed Only)  │                 │
│  └────────────────┘    └─────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  DAILY (00:05 UTC)                                          │
│  ┌────────────────┐                                         │
│  │ Snapshots      │  Point-in-time operator data            │
│  └────────────────┘                                         │
└─────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  DAILY (00:30 UTC)                                          │
│  ┌────────────────┐                                         │
│  │ Analytics      │  Risk scores, metrics, rankings         │
│  └────────────────┘                                         │
└─────────────────────────────────────────────────────────────┘
```

**Note:** Event extraction frequency is limited by The Graph subgraph indexing speed. Running more frequently than every 6 hours may result in duplicate processing without new data.

## Database Migrations

The project uses separate Alembic environments for each database:

```bash
# Events Database
make events-current              # Show current version
make events-autogen m="message"  # Generate new migration
make events-upgrade              # Apply all migrations
make events-downgrade t="-1"     # Rollback one migration

# Analytics Database
make analytics-current
make analytics-autogen m="message"
make analytics-upgrade
make analytics-downgrade t="-1"

# View migration history
make events-history
make analytics-history
```

## Development

### Project Entry Point

All Dagster definitions are loaded from `src/pipeline/definitions.py`, which imports and combines assets from both pipeline components.

### Manual Job Execution

```bash
# Process latest events
dagster job execute -j operator_state_update

# Create snapshots
dagster job execute -j operator_snapshots

# Run analytics
dagster job execute -j operator_analytics
```

## Monitoring

### Key Metrics (Dagster UI)

- Asset materialization status and timestamps
- Run duration and success rates
- Operators processed per run
- Partition coverage for time-series data

## Docker Troubleshooting

### Common Issues

**Database connection errors:**
```bash
# Check if PostgreSQL is healthy
docker-compose ps

# View PostgreSQL logs
docker-compose logs postgres

# Manually test database connection
docker exec -it eigenwatch-postgres psql -U postgres -c "\l"
```

**Container fails to start:**
```bash
# Rebuild images
docker-compose build --no-cache

# Check for port conflicts
lsof -i :3001  # Check if port 3001 is in use
lsof -i :5432  # Check if port 5432 is in use
```

**Migrations not applied:**
```bash
# Run migrations manually
docker exec -it eigenwatch-webserver bash
cd alembic/analytics && alembic upgrade head
cd ../events/migrations && alembic upgrade head
```

**View service logs:**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f dagster-webserver
docker-compose logs -f dagster-daemon
docker-compose logs -f postgres
```

## Production Deployment

### Docker Deployment (Recommended)

**On your VPS:**

```bash
# Clone repository
git clone https://github.com/EigenWatch/eigenwatch-pipeline.git
cd eigenwatch-pipeline

# Create production environment file
cp example.env .env
nano .env  # Add production values

# Start services in detached mode
docker-compose up -d

# Run migrations
docker exec -it eigenwatch-webserver bash
cd alembic/analytics && alembic upgrade head
cd ../events/migrations && alembic upgrade head
exit

# View logs
docker-compose logs -f
```

**Setup Nginx reverse proxy (optional):**

```nginx
# /etc/nginx/sites-available/dagster
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:3001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Manual Deployment (Without Docker)

```bash
# Production mode (separate processes)
dagster-daemon run &           # Handles schedules
dagster-webserver -h 0.0.0.0 -p 3001  # UI
```

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [The Graph Documentation](https://thegraph.com/docs/)
