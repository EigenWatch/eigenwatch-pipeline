# Operator Analytics Pipeline

A Dagster-based data pipeline for operator profiling and risk analysis, processing EigenLayer protocol events into actionable analytics.

## Architecture Overview

### Design Principles

- **Entity-Centric Processing**: Query changed operators via entity tables (single timestamp cursor)
- **Full State Reconstruction**: Always rebuild from complete event history (not incremental deltas)
- **SQL-First Approach**: Leverage database aggregations and window functions
- **Clear Phase Separation**: Extract → Transform → Load → Analyze

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────┐
│  PHASE 1: EXTRACTION                                    │
│  changed_operators_since_last_run                       │
│  - Query all event tables for operators with changes    │
│  - Use timestamp-based cursor (no race conditions)      │
│  - Output: Set of affected operator_ids                 │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  PHASE 2: STATE REBUILD (For Affected Operators)       │
│                                                         │
│  ├─ operator_strategy_state_asset                      │
│  │  └─ TVS, encumbered magnitude, utilization          │
│  │                                                      │
│  ├─ operator_allocations_asset                         │
│  │  └─ Current & pending allocations                   │
│  │                                                      │
│  ├─ operator_avs_relationships_asset                   │
│  │  └─ Registration history & current status           │
│  │                                                      │
│  ├─ operator_commission_rates_asset                    │
│  │  └─ Current & upcoming commission rates             │
│  │                                                      │
│  ├─ operator_delegators_asset                          │
│  │  └─ Delegator lists & per-strategy shares           │
│  │                                                      │
│  ├─ operator_slashing_incidents_asset                  │
│  │  └─ Slashing events & amounts                       │
│  │                                                      │
│  └─ operator_current_state_asset                       │
│     └─ Aggregate all metrics into main profile         │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  PHASE 3: SNAPSHOTS (Daily, conditional)               │
│                                                         │
│  ├─ operator_daily_snapshots_asset                     │
│  │  └─ Daily copy of operator_current_state            │
│  │                                                      │
│  └─ operator_strategy_daily_snapshots_asset            │
│     └─ Daily copy of operator_strategy_state           │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  PHASE 4: ANALYTICS (Daily, after snapshots)           │
│                                                         │
│  ├─ volatility_metrics_asset                           │
│  │  └─ Calculate volatility (7/30/90 day windows)      │
│  │                                                      │
│  ├─ concentration_metrics_asset                        │
│  │  └─ HHI, Gini, distribution metrics                 │
│  │                                                      │
│  └─ operator_analytics_asset                           │
│     └─ Comprehensive risk scores                       │
└─────────────────────────────────────────────────────────┘
```

## Project Structure

```
analytics_pipeline/
├── __init__.py                 # Dagster definitions
├── resources.py                # Database and config resources
├── assets/
│   ├── extraction.py           # Changed operator detection
│   ├── state_rebuild.py        # State reconstruction assets
│   ├── snapshots.py            # Daily snapshot creation
│   └── analytics.py            # Risk scoring and metrics
├── services/
│   └── state_reconstructor.py # SQL-based rebuild logic
└── tests/
    ├── test_extraction.py
    ├── test_state_rebuild.py
    └── test_analytics.py

dagster_home/
├── dagster.yaml                # Instance configuration
├── workspace.yaml              # Workspace configuration
└── .env                        # Environment variables
```

## Installation

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Two databases:
  - `eigenlayer_events` (populated by subgraph pipeline)
  - `eigenlayer_analytics` (created by this pipeline)

### Setup

1. **Clone and install:**

```bash
git clone <repository>
cd analytics_pipeline
pip install -e .
```

2. **Configure environment:**

```bash
cp .env.example .env
# Edit .env with your database URLs
```

3. **Initialize Dagster:**

```bash
export DAGSTER_HOME=/path/to/dagster_home
dagster instance migrate
```

4. **Create analytics database tables:**

```bash
# Run migration scripts to create analytics tables
python scripts/create_analytics_tables.py
```

## Usage

### Running the Pipeline

**Option 1: Dagster UI (Development)**

```bash
dagster dev
# Navigate to http://localhost:3000
# Launch jobs manually or wait for schedules
```

**Option 2: Dagster Daemon (Production)**

```bash
# Start the daemon (handles schedules)
dagster-daemon run

# In separate terminal, start the UI
dagster-webserver -h 0.0.0.0 -p 3000
```

### Schedules

The pipeline runs on three schedules:

1. **State Update Job** (`operator_state_update`)

   - **Schedule**: Every 6 hours (`0 */6 * * *`)
   - **Purpose**: Process new events and rebuild affected operator state
   - **Duration**: ~5-30 minutes (depending on changes)

2. **Snapshot Job** (`operator_snapshots`)

   - **Schedule**: Daily at 00:05 UTC (`5 0 * * *`)
   - **Purpose**: Create daily snapshots of all operator state
   - **Duration**: ~2-5 minutes

3. **Analytics Job** (`operator_analytics`)
   - **Schedule**: Daily at 00:30 UTC (`30 0 * * *`)
   - **Purpose**: Calculate risk scores and metrics
   - **Duration**: ~5-15 minutes

### Manual Execution

```bash
# Run state update for latest changes
dagster job execute -j operator_state_update

# Create snapshots for yesterday
dagster job execute -j operator_snapshots

# Calculate analytics
dagster job execute -j operator_analytics

# Backfill snapshots for date range
dagster job backfill -j operator_snapshots \
  --from 2024-01-01 --to 2024-01-31
```

## Configuration

### Database Resources

Configure in `.env`:

```bash
EVENTS_DB_URL=postgresql://user:pass@host:5432/eigenlayer_events
ANALYTICS_DB_URL=postgresql://user:pass@host:5432/eigenlayer_analytics
```

### Pipeline Settings

Adjust in `resources.py` or via environment variables:

```python
# Checkpoint
checkpoint_key: str = "analytics_pipeline_v1"

# Safety buffers (prevent race conditions)
safety_buffer_blocks: int = 10
safety_buffer_seconds: int = 60

# Performance
max_operators_per_batch: int = 100
commit_batch_size: int = 50

# Snapshots
snapshot_hour_utc: int = 0  # Midnight UTC

# Analytics
volatility_windows: list = [7, 30, 90]  # Days
min_data_points_for_analytics: int = 7
```

## Key Features

### 1. No Race Conditions

- **Problem**: Subgraph loads 100 events/batch; some blocks partially loaded
- **Solution**: Entity-centric timestamp cursor
  - Query: "Which operators had events since last run?"
  - Always rebuild full state from ALL events
  - Eventual consistency guaranteed

### 2. Idempotent Operations

- Reprocessing same operators produces same results
- Safe to re-run failed jobs
- No partial state updates

### 3. SQL-First Performance

- Bulk operations via CTEs and window functions
- Minimal application-level loops
- Database handles aggregations

### 4. Clear Dependencies

```
changed_operators → [state_assets] → current_state → snapshots → analytics
```

Dagster ensures correct execution order

### 5. Partitioned Analytics

- Daily partitions for snapshots and analytics
- Easy backfilling: `dagster job backfill --from X --to Y`
- Incremental processing of historical data

## Monitoring

### Dagster UI Metrics

- Asset materialization status
- Run duration and success rate
- Partition coverage
- Metadata (operators processed, duration, etc.)

### Custom Metrics (in logs)

```python
context.log.info(f"Processed {count} operators in {duration}s")
```

### Database Queries

```sql
-- Check pipeline lag
SELECT
  last_processed_at,
  NOW() - last_processed_at as lag
FROM pipeline_checkpoints
WHERE pipeline_name = 'analytics_pipeline_v1';

-- Count processed operators
SELECT COUNT(*)
FROM operator_current_state
WHERE updated_at > NOW() - INTERVAL '1 hour';

-- Analytics coverage
SELECT
  date,
  COUNT(*) as operators_analyzed
FROM operator_analytics
GROUP BY date
ORDER BY date DESC
LIMIT 7;
```

## Troubleshooting

### Pipeline Not Processing Operators

**Symptom**: `changed_operators_since_last_run` returns 0

**Causes**:

1. No new events in source database
2. Checkpoint timestamp too recent

**Solutions**:

```sql
-- Check latest events
SELECT MAX(created_at) FROM allocation_events;

-- Reset checkpoint to reprocess
UPDATE pipeline_checkpoints
SET last_processed_at = '2024-01-01'
WHERE pipeline_name = 'analytics_pipeline_v1';
```

### Slow State Rebuild

**Symptom**: `operator_current_state_asset` takes >30 minutes

**Causes**:

1. Too many operators changed
2. Missing database indexes

**Solutions**:

```sql
-- Check operator count
SELECT COUNT(*) FROM operators;

-- Add indexes
CREATE INDEX CONCURRENTLY idx_allocation_events_operator_created
ON allocation_events(operator_id, created_at);

-- Increase batch size
# In resources.py
max_operators_per_batch: int = 200
```

### Snapshots Not Created

**Symptom**: No rows in `operator_daily_snapshots`

**Causes**:

1. Current time < snapshot_hour_utc
2. No data in `operator_current_state`

**Solutions**:

```python
# Check snapshot hour setting
snapshot_hour_utc: int = 0  # Adjust if needed

# Verify source data
SELECT COUNT(*) FROM operator_current_state;
```

### Analytics Job Failures

**Symptom**: `operator_analytics_asset` fails with division by zero

**Causes**:

1. Insufficient historical data
2. No snapshots available

**Solutions**:

```sql
-- Check snapshot coverage
SELECT
  COUNT(DISTINCT snapshot_date) as days_of_data
FROM operator_daily_snapshots;

-- Adjust minimum data points
min_data_points_for_analytics: int = 3  # Lower threshold
```

## Development

### Running Tests

```bash
pytest tests/ -v
pytest tests/test_extraction.py -v
```

### Adding New Metrics

1. **Create new asset** in `assets/analytics.py`:

```python
@asset(
    ins={"snapshots": AssetIn("operator_daily_snapshots_asset")},
    description="New custom metric",
)
def custom_metric_asset(context, db, config, snapshots):
    # Calculate metric
    # Write to database
    return Output(rowcount)
```

2. **Add to job** in `__init__.py`:

```python
analytics_assets = [
    ...,
    custom_metric_asset,
]
```

3. **Add dependency** to risk scoring:

```python
@asset(
    ins={
        ...,
        "custom": AssetIn("custom_metric_asset"),
    }
)
def operator_analytics_asset(...):
    # Use custom metric in risk calculation
    pass
```

### Debugging SQL Queries

Enable query logging in `resources.py`:

```python
self._analytics_engine = create_engine(
    self.analytics_db_url,
    echo=True,  # Logs all SQL queries
)
```

## Performance Tuning

### Database Optimization

```sql
-- Essential indexes (already in schema)
CREATE INDEX idx_operator_current_state_updated
ON operator_current_state(updated_at);

CREATE INDEX idx_allocation_events_operator_created
ON allocation_events(operator_id, created_at);

-- Analyze tables regularly
ANALYZE operator_current_state;
ANALYZE operator_daily_snapshots;
```

### Pipeline Optimization

```python
# Increase batch sizes for bulk operations
commit_batch_size: int = 100

# Parallel processing (future enhancement)
max_concurrent_operators: int = 10

# Reduce logging in production
enable_detailed_logging: bool = False
```

## Deployment

### Production Checklist

- [ ] Configure production database URLs
- [ ] Set up database backups
- [ ] Enable Dagster monitoring
- [ ] Configure alerting (e.g., PagerDuty)
- [ ] Set resource limits (CPU, memory)
- [ ] Enable query logging for errors only
- [ ] Schedule regular vacuum/analyze
- [ ] Set up log aggregation

### Kubernetes Deployment

```yaml
# dagster-deployment.yaml (example)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dagster-webserver
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: dagster-webserver
          image: analytics-pipeline:latest
          command: ["dagster-webserver"]
          env:
            - name: EVENTS_DB_URL
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: events-url
```

## License

MIT

## Support

For issues or questions:

- GitHub Issues: <repository>/issues
- Documentation: <repository>/wiki
- Email: team@example.com
