### Discord Access Configuration

To configure Discord for bot access:

1. Create New Discord Application
  - Go to Discord Developer Portal at https://discord.com/developers/applications
  - Click New Application
  - Enter a name for the new application, accept the terms, and click Create
1. Configure Bot
  - Click on Bot in the left-hand menu
  - Edit Username if necessary
  - Click Reset Token and follow instructions to view new token
    - Back up token somewhere safe
  - Turn on the Server Members Intent
  - Turn on the Message Content Intent
  - Click Save
1. Configure Authentication
  - Click on OAuth2 in the left-hand menu
  - Select the "bot" checkbox under SCOPES
  - Under Bot Permissions, select the following permissions:
    - Read Messages/View Channels
    - Send Messages
    - Use Slash Commands
  - Copy the generated URL at the bottom of the screen
1. Authorize Bot for Server
  - Paste the URL into a browser where you are logged in as the Discord server owner
  - Select the correct server under Add to Server
  - Click Continue
  - Review permissions and click Authorize
1. Configure Allowed Channels
  - In the Discord server settings, go to the Integrations screen
  - Click on Manage for the bot
  - Remove access to All Channels
  - Add access to the allowed channel
  - Save Changes

For access to Discord channel IDs:

1. Turn on Developer Mode
  - In Discord, click on the settings icon (gear) next to your username
  - Click on Advanced in the left-hand menu
  - Turn on Developer Mode for access to channel IDs