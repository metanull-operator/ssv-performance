# ssv-performance Overview

The ssv-performance repository contains the ssv-performance-bot and related tools for providing SSV operator performance data via Discord. The ssv-performance-bot posts daily performance updates to a configured Discord channel and responds to user commands in that channel and in direct messages.

Components of this repository include:
- ssv-performance-bot - Discord bot that posts performance data and responds to commands
- ssv-performance-collector - Script to collect performance data from a third-party API and store in the ClickHouse database
- ssv-performance-sheets - Script to upload performance data to Google Sheets
- ssv-validator-count-sheets - Script to upload performance data to Google Sheets
- clickhouse-import - Script to import performance data SQL into the database

# ssv-performance Installation and Configuration

Clone the ssv-performance repository into your preferred directory.

```
git clone https://github.com/metanull-operator/ssv-performance
cd ssv-performance
```

## Configure ssv-performance-bot & ClickHouse

The ssv-performance-bot and ClickHouse database run togther in a docker compose application.

### Configure .env

Copy the sample `.env` file and use your preferred text editor to modify it. Here we will use `vi`.

```
cp .env.sample .env
vi .env
```

Set the following environment variables in `.env`:

- BOT_DISCORD_CHANNEL_ID - A single Discord channel ID on which the bot will respond to commands and post daily messages. You can find the Discord channel ID by right-clicking on the channel name and selecting `Copy Channel ID`. [Turn on Discord Developer Mode](docs/discord-developer.md) if you do not see the `Copy Channel ID` option.
- BOT_ALERT_RECENTLY_REMOVED_ROLE - A single Discord Role ID that the bot will @mention when operators are removed from SSV API responses.

Other environment variables in `.env` may be modified as necessary to suit your implementation.

Save and exit the text editor.

### Store Discord Token

The ssv-performance-bot requires permission for Discord channel access and a token for API access. Generate the Discord token and put the contents in `credentials/discord-token.txt`. See our [Discord token documentation](docs/discord-token.md) for details on configuring Discord API access and generating a Discord token.

```bash
vi credentials/discord-token.txt
```

Enter the Discord token, then save and exit.

### Store ClickHouse Password

A password for the ClickHouse database must be stored in `credentials/clickhouse-password.txt`. This password will be used as the initial password for the `ssv_performance` ClickHouse user. The password cannot be left blank.

```bash
vi credentials/clickhouse-password.txt
```

Enter your new ClickHouse password, then save and exit.

### Build ssv-performance-bot Image

Build the ssv-performance-bot Docker image.

```bash
docker build -t ssv-performance-bot ./ssv-performance-bot
```

### Start the Applications

To start the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance up -d
```

To stop the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance down
```

## Daily Alert Sections

The bot's daily alert post to Discord (also produced on demand by the `/alerts` command) contains several sections. Each one is rendered only when it has content:

- **24h < *threshold*** - Operators whose 24h performance fell below one of the configured alert thresholds.
- **30d < *threshold*** - Operators whose 30d performance fell below one of the configured alert thresholds.
- **Removed Operators with Active Validators** - Operators whose record has not been updated by the collector within the last 36 hours but who still have active validators. This is a short-term "heads up" indicator recomputed on every query.
- **Recently Removed Operator(s)** - Operators whose verified status was removed by the collector's staleness sweep within the last `VO_RECENTLY_REMOVED_DAYS` days (default 3). Each entry lists the date of demotion so a missed daily post does not cause the notification to be lost entirely. When this section fires, the bot also @-mentions the Discord role configured by `BOT_ALERT_RECENTLY_REMOVED_ROLE` (role ID preferred, role name accepted as a fallback; empty to disable).

## clickhouse-import.sh

See the [clickhouse-import README](scripts/clickhouse-import/README.md) for the steps to import performance data SQL files. This may be used to restore backups or to transfer a database from one ClickHouse instance to another.

## ssv-performance-collector

ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores it in the ClickHouse database. Create a daily cronjob for each network for which you want to collect performance data. The ClickHouse database must be running to collect performance data.

See the [ssv-performance-collector README](scripts/ssv-performance-collector/README.md) for more details.

## ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. Each Google Sheet will contain the performance data for a single Ethereum network and performance period (24h/30d). It is recommended to run this script daily as a cronjob. A separate instance must be run for each Ethereum network and performance period copied to Google Sheets.

See the [ssv-performance-sheets README](scripts/ssv-performance-sheets/README.md) for more details.

## ssv-validator-count-sheets

ssv-validator-count-sheets copies SSV validator count data from the ClickHouse database to a Google Sheet. Each Google Sheet will contain the validator count data for a single Ethereum network. It is recommended to run this script daily as a cronjob. A separate instance must be run for each Ethereum network copied to Google Sheets.

See the [ssv-validator-count-sheets README](scripts/ssv-validator-count-sheets/README.md) for more details.

## clickhouse-export.sh

See the [clickhouse-export README](scripts/clickhouse-export/README.md) for the steps to export performance data SQL files from the ClickHouse database for backups or to import into another ClickHouse database instance.