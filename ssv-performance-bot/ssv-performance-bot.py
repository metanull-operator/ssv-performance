import sys
import argparse
import asyncio
import logging
import os
import discord
import vo_performance_bot.vopb_commands as vopb_commands
from discord.ext import commands
from storage.storage_factory import StorageFactory
from vo_performance_bot.vopb_loops import LoopTasks

# Configure logging with a default level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_arguments():

    raw_dm_env = os.environ.get("BOT_DM_RECIPIENTS", "")
    default_dm_list = [r for r in raw_dm_env.split(",") if r.strip()]

    parser = argparse.ArgumentParser(description="SSV Verified Operator Committee Discord bot")

    parser.add_argument("-n", "--network", default=os.environ.get("NETWORK", "mainnet"))
    parser.add_argument("-d", "--discord_token_file", default=os.environ.get("BOT_DISCORD_TOKEN_FILE"))
    parser.add_argument("-t", "--alert_time", default=os.environ.get("BOT_DAILY_MESSAGE_TIME", "14:00"))
    parser.add_argument("-c", "--channel_id", default=os.environ.get("BOT_DISCORD_CHANNEL_ID"))
    parser.add_argument("-e", "--extra_message", default=os.environ.get("BOT_EXTRA_MESSAGE"))
    parser.add_argument("--dm_recipients", nargs="*", default=default_dm_list)
    parser.add_argument("--ch_operators_table", default=os.environ.get("CH_PERFORMANCE_TABLE", "operators"))
    parser.add_argument("--ch_performance_table", default=os.environ.get("CH_PERFORMANCE_TABLE", "performance"))
    parser.add_argument("--ch_subscriptions_table", default=os.environ.get("CH_SUBSCRIPTIONS_TABLE", "subscriptions"))
    parser.add_argument("--log_level", default=os.environ.get("BOT_LOG_LEVEL", "INFO"),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")

    args = parser.parse_args() 

    try:
        dm_recipients = list(map(int, args.dm_recipients)) if args.dm_recipients else []
    except ValueError:
        raise ValueError(f"Invalid ID(s) in --dm_recipients: {args.dm_recipients}")

    return args.network, args.discord_token_file, args.channel_id, args.alert_time, args.extra_message, args.ch_operators_table, args.ch_performance_table, args.ch_subscriptions_table, dm_recipients, args.log_level

def read_discord_token_from_file(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            return file.read().strip()
    except Exception as e:
        logging.error(f"Unable to retrieve Discord token: {e}", exc_info=True)
        sys.exit(1)

async def main():
    try:
        network, discord_token_file, channel_id, alert_time, extra_message, operators_data_table, performance_data_table, subscription_data_table, dm_recipients, log_level = parse_arguments()
    except SystemExit as e:
        if e.code != 0:
            logging.error("Argument parsing failed", exc_info=True)
        sys.exit(e.code)

    # Set logging level dynamically
    logging.getLogger().setLevel(log_level.upper())
    logging.info(f"Logging level set to {log_level.upper()}")

    try:
        StorageFactory.initialize('ssv_performance', 'ClickHouse')
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
        logging.info(f'Logged in as {bot.user.name}')
        logging.info(f"Getting channel {channel_id}")

        try:
            channel = bot.get_channel(int(channel_id))
            if not channel:
                logging.error(f"Cannot get channel {channel_id}")
                sys.exit(1)

            loop_tasks = LoopTasks(network, bot, channel, alert_time, extra_message, dm_recipients)
            bot.loop.create_task(loop_tasks.start_tasks())
            logging.info("Loop tasks started successfully.")
        except Exception as e:
            logging.error(f"Error in on_ready event: {e}", exc_info=True)
            sys.exit(1)

    try:
        await vopb_commands.setup(network, bot, channel_id, extra_message)
        logging.info("Commands setup successfully.")
    except Exception as e:
        logging.error(f"Error setting up commands: {e}", exc_info=True)
        sys.exit(1)

    try:
        discord_token = read_discord_token_from_file(discord_token_file)
        await bot.start(discord_token)
    except Exception as e:
        logging.error(f"Error starting bot: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())