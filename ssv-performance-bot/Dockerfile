FROM python:3.11-slim

# Build-time args for UID/GID
ARG PROCESS_UID=30001
ARG PROCESS_GID=30001
ARG PROCESS_USER=bot
ARG PROCESS_GROUP=bot

# Install dependencies and gosu for privilege dropping
RUN apt-get update && apt-get install -y --no-install-recommends \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user and group safely
RUN set -eux; \
    if ! getent group "${PROCESS_GID}" >/dev/null; then \
        groupadd -g "${PROCESS_GID}" "${PROCESS_GROUP}"; \
    fi; \
    if ! id -u "${PROCESS_UID}" >/dev/null 2>&1; then \
        useradd -u "${PROCESS_UID}" -g "${PROCESS_GID}" -m -s /bin/bash "${PROCESS_USER}"; \
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
ENV DISCORD_TOKEN_FILE=/etc/ssv-performance-bot/discord-token.txt
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

# Entrypoint handles privilege drop
ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "ssv-performance-bot.py"]
