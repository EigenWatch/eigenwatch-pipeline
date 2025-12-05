FROM python:3.11-slim

# 1. Install System Dependencies
# 'make' is required for your migration commands.
# 'curl' is useful for healthchecks.
RUN apt-get update && apt-get install -y \
    make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Install 'uv' Package Manager
# We copy the binary directly from the official uv image (Best Practice)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 3. Set Working Directory
WORKDIR /app

# 4. Install Python Dependencies
# We copy only the dependency files first to leverage Docker caching.
# If pyproject.toml hasn't changed, this step is skipped on rebuilds.
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment at /app/.venv
# --frozen: ensures we strictly follow the lock file
# --no-install-project: installs dependencies only, not your code yet
RUN uv sync --frozen --no-install-project --no-dev

# 5. Add Virtual Env to PATH
# This ensures 'dagster', 'alembic', and 'make' use the uv-installed python
ENV PATH="/app/.venv/bin:$PATH"

# 6. Copy Application Code
COPY . .

# 7. Install the Project
# Now we install the actual project code
RUN uv sync --frozen --no-dev

# 8. Setup Startup Script
# Copy the script we created above and make it executable
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# 9. Set Dagster Home
# Dagster looks for dagster.yaml in this directory
ENV DAGSTER_HOME=/app

# 10. Expose Port & Run
EXPOSE 3005
CMD ["./start.sh"]