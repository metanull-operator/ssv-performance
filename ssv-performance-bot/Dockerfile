FROM python:3.11-slim

# Build-time args for UID/GID
ARG BOT_UID=30001
ARG BOT_GID=30001
ARG BOT_USER=bot
ARG BOT_GROUP=bot

# Install dependencies and gosu for privilege dropping
RUN apt-get update && apt-get install -y --no-install-recommends \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user and group safely
RUN set -eux; \
    if ! getent group "${BOT_GID}" >/dev/null; then \
        groupadd -g "${BOT_GID}" "${BOT_GROUP}"; \
    fi; \
    if ! id -u "${BOT_UID}" >/dev/null 2>&1; then \
        useradd -u "${BOT_UID}" -g "${BOT_GID}" -m -s /bin/bash "${BOT_USER}"; \
    fi

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY . .

# Copy and configure entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Environment variables
ENV BOT_EXTRA_MESSAGE="/subscribe for @mentions on daily alerts or for performance report DMs. /operator for latest performance data. /help for more info."
ENV CLICKHOUSE_PASSWORD_FILE=/etc/ssv-performance-bot/clickhouse-password.txt
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

# Entrypoint handles privilege drop
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "ssv-performance-bot.py"]
