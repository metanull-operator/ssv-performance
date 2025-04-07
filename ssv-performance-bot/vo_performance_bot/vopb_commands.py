import traceback
import logging
from collections import defaultdict
import copy
from discord.commands import Option
from storage.storage_factory import StorageFactory
from vo_performance_bot.vopb_messages import (
    create_subscriptions_message,
    send_operator_performance_messages,
    respond_vo_threshold_messages,
    send_direct_message_test
)


# Determine whether the Discord context is a channel or not. Could be DMs.
def is_channel(ctx):
    return hasattr(ctx, 'guild') and ctx.guild


async def setup(network, bot, allowed_channel_id, extra_message):

    @bot.slash_command(name="help", description="Shows help information")
    async def help(ctx):

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        help_text = """
The VO Performance Bot delivers SSV operator performance data via Discord through channels and direct messages.

- Daily scheduled channel message listing operators that do not meet various performance thresholds
- Daily scheduled direct messages listing recent performance for subscribed operator IDs
- Channel/direct message command listing operators that do not meet various performance thresholds
- Channel/direct message command listing recent performance for listed operator IDs

Subscribe to daily direct messages or threshold alert @mentions:
- daily - Subscriptions to operator ID daily operator performance direct messages will cause you to receive a direct message once per day listing recent performance for subscribed operator IDs
- alerts - Subscriptions to operator ID alerts will cause you to receive an @mention when any of your subscribed operator IDs is listed in the daily threshold alerts message.

Commands may be run in the designated channel(s) or by direct message to the bot.
Performance data is taken from a daily snapshot of 24-hour SSV operator performance.
Thresholds displayed are subject to change.

**Commands:**
- /alerts: List all operator IDs whose recent performance is below various alert thresholds
- /operator [operator_ids...]: Show recent performance for specified operator IDs
- /subscribe daily|alerts [operator_ids...]: Subscribe to daily operator performance direct messages or threshold alert @mentions
- /unsubscribe daily|alerts [operator_ids...]: Unsubscribe from daily operator performance direct messages or threshold alert @mentions
- /subscriptions: List all operator IDs to which you are subscribed for daily operator performance messages or threshold alert @mentions
- /info: Display bot information
- /help: Shows this help message
        """
        await ctx.respond(help_text)


    async def send_user_subscriptions(ctx, ephemeral, followup):
        try:
            storage = StorageFactory.get_storage('ssv_performance')
            subscriptions = storage.get_subscriptions_by_userid(network, ctx.author.id)
            logging.debug(f"User subscriptions: {subscriptions}")
            message = create_subscriptions_message(subscriptions, ctx.author).strip()
            if message:
                if not followup:
                    await ctx.respond(message, ephemeral=ephemeral)
                else:
                    await ctx.send_followup(message, ephemeral=ephemeral)
        except Exception as e:
            logging.error(f"Error sending user subscriptions: {e}", exc_info=True)
            traceback.print_exc()


    def allowed_channel(ctx):
        # Messages will be allowed if the channel is an allowed channel, or if we aren't even on a channel (DMs)
        return not is_channel(ctx) or str(ctx.channel.id) == allowed_channel_id


    @bot.slash_command(name='subscriptions', description='List all operator IDs subscribed for daily performance direct messages or threshold alert @mentions')
    async def subscriptions(ctx):
        logging.info("/subscriptions called")
        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return
        await send_user_subscriptions(ctx, ephemeral=is_channel(ctx), followup=False)


    @bot.slash_command(name='subscribe', description='Subscribe to daily operator performance direct messages or threshold alert @mentions')
    async def subscribe(ctx, notification_type: Option(str, "Choose notification type", choices=['daily', 'alerts']),
                        operator_ids: Option(str, "Enter operator IDs separated by spaces")):

        logging.info("/subscribe called")

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        # Split operator IDs and filter to integers. Error if list is empty afterward.
        operator_ids = [int(op_id) for op_id in operator_ids.split() if op_id.isdigit()]
        if not operator_ids:
            await ctx.respond("Error: Operator IDs must be positive integers.", ephemeral=True)
            return

        # Track responded so we know whether future messages are follow-ups
        responded = False

        # Attempt a DM to user. Immediately let user know if DMs failed.
        test_msg = "You have requested to receive direct message alerts regarding SSV operator performance. This message confirms that you can receive direct messages from the VO Performance Bot."
        if not await send_direct_message_test(bot, ctx.author.id, test_msg.strip()):
            await ctx.respond(
                "An attempt to send you a Discord direct message has failed. This may mean that your direct messages are not open to the bot. Your subscriptions will still be added, but you may not receive daily performance direct messages.",
                ephemeral=True)
            responded = True

        try:
            storage = StorageFactory.get_storage('ssv_performance')

            # Individually store user subscriptions
            for op_id in operator_ids:
                logging.info(f"User {ctx.author.id} subscribing to {op_id} for {notification_type} notifications.")
                storage.add_user_subscription(network, ctx.author.id, op_id, notification_type)

            # Notify user of updated status
            if not responded:
                await ctx.respond("Your subscriptions have been updated.", ephemeral=False)
                responded = True
            else:
                await ctx.send_followup("Your subscriptions have been updated.", ephemeral=False)

            logging.info(f"User {ctx.author.id} subscribed to {operator_ids} for {notification_type} notifications.")

            # Send complete list of current subscriptions
            await send_user_subscriptions(ctx, ephemeral=is_channel(ctx), followup=responded)

        except Exception as e:
            logging.error(f"Error subscribing user: {e}", exc_info=True)
            if not responded:
                await ctx.respond("An error occurred while updating your subscriptions.", ephemeral=True)
            else:
                await ctx.send_followup("An error occurred while updating your subscriptions.", ephemeral=True)


    @bot.slash_command(name='unsubscribe', description='Unsubscribe from daily operator performance direct messages or threshold alert @mentions')
    async def unsubscribe(ctx, notification_type: Option(str, "Choose notification type", choices=['daily', 'alerts']),
                          operator_ids: Option(str, "Enter operator IDs separated by spaces")):

        logging.info("/unsubscribe called")

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        # Split operator IDs and filter to integers. Error if list is empty afterward.
        operator_ids = [int(op_id) for op_id in operator_ids.split() if op_id.isdigit()]
        if not operator_ids:
            await ctx.respond("Operator IDs must be positive integers.", ephemeral=True)
            return

        # Track responded so we know whether future messages are follow-ups
        responded = False

        try:
            storage = StorageFactory.get_storage('ssv_performance')

            # Individually delete user subscriptions
            for op_id in operator_ids:
                storage.del_user_subscription(network, ctx.author.id, op_id, notification_type)

            await ctx.respond("Your subscriptions have been updated.", ephemeral=False)
            responded = True

            # Send complete list of current subscriptions
            await send_user_subscriptions(ctx, ephemeral=is_channel(ctx), followup=responded)

        except Exception as e:
            logging.error(f"Error unsubscribing user: {e}")
            if not responded:
                await ctx.respond("An error occurred while updating your subscriptions.", ephemeral=True)
            else:
                await ctx.send_followup("An error occurred while updating your subscriptions.", ephemeral=True)


    @bot.slash_command(name='operator', description='Show recent operator performance for listed operator IDs')
    async def operator(ctx, operator_ids: Option(str, "Enter operator IDs separated by spaces")):

        logging.info("/operator called")

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        # Split operator IDs and filter to integers. Error if list is empty afterward.
        operator_ids_list = [int(op_id) for op_id in operator_ids.split() if op_id.isdigit()]
        if not operator_ids_list:
            await ctx.respond("Operator IDs must be positive integers.", ephemeral=False)
            return

        try:
            storage = StorageFactory.get_storage('ssv_performance')
            perf_data = storage.get_performance_by_opids(network, operator_ids_list)

            if not perf_data:
                logging.info(f"operator() perf_data empty for {operator_ids} [077003]")
                await ctx.respond("Performance data not available.", ephemeral=False)
                return

            await send_operator_performance_messages(perf_data, ctx, operator_ids_list)
        except Exception as e:
            logging.error(f"Error fetching operator performance: {e}", exc_info=True)
            await ctx.respond("An error occurred while fetching operator performance data.", ephemeral=True)

    def merge_operator_performance(dict1, dict2):
        merged = {}

        for k in set(dict1) | set(dict2):
            merged[k] = copy.deepcopy(dict1.get(k, {}))

            d2 = dict2.get(k, {})
            for key, value in d2.items():
                if isinstance(value, dict) and key in merged[k] and isinstance(merged[k][key], dict):
                    merged[k][key].update(value)
                else:
                    merged[k][key] = value

        return merged


    @bot.slash_command(name='alerts', description='List all operators whose recent performance is below various alert thresholds')
    async def alerts(ctx):

        logging.info("/alerts called")

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        await ctx.defer()

        try:
            storage = StorageFactory.get_storage('ssv_performance')
            perf_data_24h = storage.get_latest_performance_data(network, '24h')
            perf_data_30d = storage.get_latest_performance_data(network, '30d')

            perf_data = merge_operator_performance(perf_data_24h, perf_data_30d)

            if not perf_data:
                logging.error(f"alerts() perf_data empty [077001]")
                await ctx.followup.send("Performance data not available.", ephemeral=True)
                return

            await respond_vo_threshold_messages(ctx, perf_data, extra_message=extra_message)

        except Exception as e:
            logging.error(f"Error fetching alerts: {e}", exc_info=True)
            await ctx.followup.send("An error occurred while fetching alerts.", ephemeral=True)


    @bot.slash_command(name='info', description='Display bot information')
    async def info(ctx):

        logging.info("/info called")

        if not allowed_channel(ctx):
            await ctx.respond("VO Performance Bot commands are not allowed in this channel.", ephemeral=True)
            return

        await ctx.defer()

        try:
            storage = StorageFactory.get_storage('ssv_performance')

            latest_date = storage.get_latest_perf_data_date(network)

            hello = "Hello! This is VO Performance Bot!\n"
            if latest_date:
                hello += f"Results are from a snapshot of 24h performance last collected on {latest_date}."
            else:
                logging.error("Could not retrieve latest data point date. [077002]")
                hello += "Results are from a snapshot of 24h performance."

            await ctx.followup.send(hello, ephemeral=False)

        except Exception as e:
            logging.error(f"Error fetching bot information: {e}", exc_info=True)
            await ctx.followup.send("An error occurred while fetching bot information.", ephemeral=True)


    @bot.event
    async def on_command_error(ctx, error):

        if isinstance(error, commands.CommandNotFound):
            # If the command is not found, do nothing
            return

        # Handle other errors as you see fit
        logging.error(f"Error in command {ctx.command}: {error}")
        await ctx.respond(f"An error occurred: {error}", ephemeral=True)
