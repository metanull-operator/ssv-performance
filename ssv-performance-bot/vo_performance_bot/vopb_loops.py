import logging
from datetime import datetime, timedelta
from discord.ext import tasks
from storage.storage_factory import StorageFactory
from vo_performance_bot.vopb_messages import send_daily_direct_messages, send_vo_threshold_messages
import asyncio


class LoopTasks:

    def __init__(self, network, bot, channel, notification_time_str, extra_message, dm_recipients=[], mentions_30d=False):
        self.network = network
        self.bot = bot
        self.extra_message = extra_message
        self.channel = channel
        self.notification_time_str = notification_time_str
        self.notification_time = datetime.strptime(notification_time_str, "%H:%M").time()
        self.dm_recipients = dm_recipients
        self.mentions_30d = mentions_30d


    async def start_tasks(self):
        now = datetime.now()
        target = datetime.combine(now.date(), self.notification_time).replace(second=0, microsecond=0)

        logging.debug(f"Current time: {now}, Target time: {target}")

        if self.daily_notification_task.is_running():
            logging.warning("daily_notification_task is already running. Skipping start.")
        if self.performance_status_all_loop.is_running():
            logging.warning("performance_status_all_loop is already running. Skipping start.")

        if self.daily_notification_task.is_running() and self.performance_status_all_loop.is_running():
            logging.warning("All expected tasks running. Skipping start delay and task starts.")
            return

        if now >= target + timedelta(minutes=1):
            logging.debug("Target time has passed. Scheduling for tomorrow.")
            target += timedelta(days=1)

        delay = (target - now).total_seconds()
        logging.info(f"Delaying for {delay} seconds until first loop run at {target}...")
        await asyncio.sleep(delay)

        if not self.daily_notification_task.is_running():
            self.daily_notification_task.start()
        if not self.performance_status_all_loop.is_running():
            self.performance_status_all_loop.start()


    @tasks.loop(hours=24)
    async def daily_notification_task(self):

        logging.info(f"Sending daily direct messages: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            sub_storage = StorageFactory.get_storage('ssv_performance')
            subscriptions = sub_storage.get_subscriptions_by_type(self.network, 'daily')

            if not subscriptions:
                logging.warning("Subscription data empty in daily_notification_task()")
                return

            op_ids = list(subscriptions.keys())

            perf_storage = StorageFactory.get_storage('ssv_performance')
            perf_data = perf_storage.get_performance_by_opids(self.network, op_ids)

            if not perf_data:
                logging.warning(f"Performance data empty for {op_ids} in daily_notification_task()")
                return

            await send_daily_direct_messages(self.bot, perf_data, subscriptions, self.dm_recipients)

        except Exception as e:
            logging.error(f"{type(e).__name__} exception in daily_notification_task(): {e}", exc_info=True)


    @tasks.loop(hours=24)
    async def performance_status_all_loop(self):
        logging.info(f"Sending alert message to channel: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            storage = StorageFactory.get_storage('ssv_performance')
            perf_data_24h = storage.get_latest_performance_data(network, '24h')
            perf_data_30d = storage.get_latest_performance_data(network, '30d')

            perf_data = merge_operator_performance(perf_data_24h, perf_data_30d)

            if not perf_data:
                logging.warning("Performance data unavailable.")
                return

            subscriptions = storage.get_subscriptions_by_type(self.network, 'alerts')

            if not subscriptions:
                logging.warning("Subscription data unavailable.")

            mention_periods = ['24h', '30d'] if self.mentions_30d else ['24h']

            await send_vo_threshold_messages(self.channel, perf_data, extra_message=self.extra_message,
                                             subscriptions=subscriptions, mention_periods=mention_periods)
        except Exception as e:
            logging.error(f"{type(e).__name__} exception in performance_status_all_loop(): {e}", exc_info=True)

            try:
                # Attempt to notify the Discord channel about the error
                channel = self.bot.get_channel(self.channel_id)
                if channel:
                    await channel.send(f"An error has occurred attempting to send daily alert messages.")
            except Exception as send_e:
                logging.error(f"{type(send_e).__name__} exception attempting to notify channel of exception in performance_status_all_loop(): {send_e}", exc_info=True)
