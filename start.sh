#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# ------------------------------------------------
# 1. Run Database Migrations
# ------------------------------------------------
echo "🚀 Starting Deployment Migrations..."

echo "--> Upgrading Events Database..."
make events-upgrade t="head"

echo "--> Upgrading Analytics Database..."
make analytics-upgrade t="head"

echo "✅ Migrations completed successfully."

# ------------------------------------------------
# 2. Start Dagster Daemon (Background Process)
# ------------------------------------------------
# This handles schedules and sensors. 
# We use '&' to put it in the background so the script continues.
echo "--> Starting Dagster Daemon..."
dagster-daemon run &

# ------------------------------------------------
# 3. Start Dagster Webserver (Foreground Process)
# ------------------------------------------------
# This must be the last command and run in foreground to keep the container alive.
echo "--> Starting Dagster Webserver on port 3005..."
dagster-webserver -h 0.0.0.0 -p 3005