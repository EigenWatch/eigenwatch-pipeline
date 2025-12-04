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

- Python 3.9-3.13
- PostgreSQL 12+
- The Graph API access

### Installation

**Using uv (recommended):**

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate     # Windows

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

# Then run migrations and start Dagster as above
```

### 3. **Create `.python-version`** (optional but recommended)

Create a new file `.python-version` in the project root:

```
3.12
```

This tells `uv` which Python version to use.

### 4. **Update `.gitignore`**

Add uv-specific entries if not already present:

```
# uv
.venv/
uv.lock
.python-version
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

## Production Deployment

```bash
# Production mode (separate processes)
dagster-daemon run &           # Handles schedules
dagster-webserver -h 0.0.0.0  # UI on port 3000
```

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [The Graph Documentation](https://thegraph.com/docs/)
