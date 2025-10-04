import sys
import argparse
import asyncio
import logging
import os
import discord
import bot.bot_commands as bot_commands
from discord.ext import commands
from storage.storage_factory import StorageFactory
from bot.bot_loops import LoopTasks
from common.config import DEFAULT_NUMBER_OF_SEGMENTS


loop_tasks = None

# Initialize logging with a default level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_arguments():

    raw_dm_env = os.environ.get("BOT_DM_RECIPIENTS", "")
    default_dm_list = [r for r in raw_dm_env.split(",") if r.strip()]

    parser = argparse.ArgumentParser(description="SSV Verified Operator Committee Discord bot")

    parser.add_argument('--mentions_30d', action='store_true')
    parser.add_argument("--network", default=os.environ.get("NETWORK", "mainnet"))
    parser.add_argument("--clickhouse_password_file", default=os.environ.get("CLICKHOUSE_PASSWORD_FILE", ""))
    parser.add_argument("--discord_token_file", default=os.environ.get("DISCORD_TOKEN_FILE", ""))
    parser.add_argument("--alert_time", default=os.environ.get("BOT_DAILY_MESSAGE_TIME", "14:00"))
    parser.add_argument("--channel_id", default=os.environ.get("BOT_DISCORD_CHANNEL_ID"))
    parser.add_argument("--extra_message", default=os.environ.get("BOT_EXTRA_MESSAGE"))
    parser.add_argument("--dm_recipients", nargs="*", default=default_dm_list)
    parser.add_argument("--log_level", default=os.environ.get("BOT_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")

    args = parser.parse_args() 

    try:
        dm_recipients = list(map(int, args.dm_recipients)) if args.dm_recipients else []
    except ValueError:
        raise ValueError(f"Invalid ID(s) in --dm_recipients: {args.dm_recipients}")

    return args.network, args.discord_token_file, args.channel_id, args.alert_time, args.extra_message, dm_recipients, args.log_level, args.mentions_30d, args.clickhouse_password_file


def read_discord_token_from_file(token_file_path):
    with open(token_file_path, 'r') as file:
        return file.read().strip()


def read_clickhouse_password_from_file(password_file_path):
    with open(password_file_path, 'r') as file:
        return file.read().strip()


async def main():
    try:
        network, discord_token_file, channel_id, alert_time, extra_message, dm_recipients, log_level, mentions_30d, clickhouse_password_file = parse_arguments()
    except SystemExit as e:
        if e.code != 0:
            logging.error("Argument parsing failed", exc_info=True)
        sys.exit(e.code)

    # Reset logging level dynamically
    logging.getLogger().setLevel(log_level.upper())
    logging.info(f"Logging level set to {log_level.upper()}")

    logging.info(f"Daily alert time: {alert_time}")

    try:
        clickhouse_password = read_clickhouse_password_from_file(clickhouse_password_file)
    except Exception as e:
        logging.info("Unable to retrieve ClickHouse password from file, trying environment variable instead.")
        clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD")

    try:
        StorageFactory.initialize('ssv_performance', 'ClickHouse', password=clickhouse_password)
        logging.info("Storage initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing storage: {e}", exc_info=True)
        sys.exit(1)

    intents = discord.Intents.default()
    intents.members = True
    intents.message_content = True

    bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

    @bot.event
    async def on_ready():
        global loop_tasks
        
        await bot.sync_commands()

        logging.info(f'Logged in as {bot.user.name}')
        logging.info(f"Getting channel {channel_id}")

        try:
            channel = bot.get_channel(int(channel_id))
            if not channel:
                logging.error(f"Cannot get channel {channel_id}")
                sys.exit(1)

            if loop_tasks is None:
                loop_tasks = LoopTasks(network, bot, channel, alert_time, extra_message, dm_recipients, mentions_30d)
                bot.loop.create_task(loop_tasks.start_tasks())
                logging.info("Loop tasks started successfully.")
            else:
                logging.info("Loop tasks already initialized, skipping restart.")
        except Exception as e:
            logging.error(f"Error in on_ready event: {e}", exc_info=True)
            sys.exit(1)

    num_segments = os.environ.get("NUMBER_OF_SEGMENTS", DEFAULT_NUMBER_OF_SEGMENTS)

    try:
        await bot_commands.setup(network, bot, channel_id, extra_message, num_segments=num_segments)
        logging.info("Commands setup successfully.")
    except Exception as e:
        logging.error(f"Error setting up commands: {e}", exc_info=True)
        sys.exit(1)

    try:
        discord_token = read_discord_token_from_file(discord_token_file)
    except Exception as e:
        logging.info("Unable to retrieve Discord token from file, trying environment variable instead.")
        discord_token = os.environ.get("DISCORD_TOKEN")

    try:    
        await bot.start(discord_token)
    except Exception as e:
        logging.error(f"Error starting bot: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())