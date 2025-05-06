# ssv-performance Overview

The ssv-performance repository contains the ssv-performance-bot and related tools for providing SSV operator performance data via Discord. The ssv-performance-bot posts daily performance updates to a configured Discord channel and responds to user commands in that channel and in direct messages.

Components of this repository include:
- ssv-performance-bot - Discord bot that posts performance data and responds to commands
- ssv-performance-collector - Script to collect performance data from a third-party API and store in the ClickHouse database
- ssv-performance-sheets - Script to upload performance data to Google Sheets
- clickhouse-import - Script to import performance data SQL into the database
- migration-dyndb-clickhouse - Deprecated script to migrate data from the legacy AWS DynamoDB database to the ClickHouse database

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

Other environment variables in `.env` may be modified as necessary to suit your implementation.

Save and exit the text editor.

### Secure the credentials/ directory

The `credentials/` directory will store sensitive password and credential information. Ensure that this directory is readable only to the user account that will run the ssv-performance-bot and associated scripts.

```bash
chmod 640 credentials
chown <USERNAME>:<GROUP> credentials
```

Replace <USERNAME> and <GROUP> with the username and group that the Docker containers will run as.

### Store Discord Token

The ssv-performance-bot requires permission for Discord channel access and a token for API access. Generate the Discord token and put the contents in `credentials/discord-token.txt`. See our [Discord token documentation](docs/discord-token.md) for details on configuring Discord API access and generating a Discord token.

```
vi credentials/discord-token.txt
```

Enter the Discord token, then save and exit.

### Store ClickHouse Password

A password for the ClickHouse database must be stored in `credentials/clickhouse-password.txt`. This password will be used as the initial password for the `ssv_performance` ClickHouse user. The password cannot be left blank.

```
vi credentials/clickhouse-password.txt
```

Enter your new ClickHouse password, then save and exit.

### Build ssv-performance-bot Image

Build the ssv-performance-bot Docker image.

```
docker build -t ssv-performance-bot:latest ./ssv-performance-bot
```

### Start the Applications

To start the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance up --build -d
```

To stop the ssv-performance-bot and ClickHouse database:

```bash
docker compose -p ssv-performance down
```

## Data Migration

If you are migrating performance data from the legacy AWS DynamoDB database, please [see the migration-dyndb-clickhouse](scripts/migration-dyndb-clickhouse/README.md) documentation.

If you are migrating performance data from another ClickHouse database instance, please [see the clickhouse-import](scripts/clickhouse-import/README.md) documentation.

## ssv-performance-collector

ssv-performance-collector gathers SSV network performance data from api.ssv.network and stores that data in the ClickHouse database. The ClickHouse database must be running to collect performance data. It is recommended to run this script daily as a cronjob. A separate instance must be run for each Ethereum network for which data is being collected.

See the [ssv-performance-collector documentation](scripts/ssv-performance-collector/README.md) for more details.

## ssv-performance-sheets

ssv-performance-sheets copies SSV performance data from the ClickHouse database to a Google Sheet. Each Google Sheets will contain the performance data for a single Ethereum network and performance period (24h/30d). It is recommended to run this script daily as a cronjob. A separate instance must be run for each Ethereum network and performance period copied to Google Sheets.

See the [ssv-performance-sheets documentation](scripts/ssv-performance-sheets/README.md) for more details.