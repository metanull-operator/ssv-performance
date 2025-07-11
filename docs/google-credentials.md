### Google Credential Creation

#### Create Google Sheets Project
- Go to Google Cloud Console (console.cloud.google.com)
- Create a new project by clicking on the New Project option in the projects drop-down in the upper left
- You may have to click the Create Project link on the next page
- Enter Project Name, as in "VOC Performance Data"
- Click Create Project

#### Enable Google Sheets API
- Open project by clicking Select Project that appears on the Notifications pop-up
- Click on API and Services in the left-hand nav
- Click on Enabled APIs and Services in the top nav. You should end up at a screen that says "Welcome to the API Library"
- Search for Google Sheets API, which should be under Google Workspaces
- Click Enable

#### Create Credentials
- Click Create Credentials
- Select Application Data
- Click Next
- Add Service Account Name, such as "SSV Performance Collector"
- Service Account ID should auto-complete based on Account Name
- Make a copy of the Account ID (looks like an email address). You will use this to grant access to the Google Sheet to the service account.
- Add a Service Account Description, such as "Updates Google Sheet containing SSV performance data"
- Click Create and Continue
- Click Done
- Click on the new service account in the list
- Click on Keys menu at top
- Click on Add Key then Create New Key
- Select JSON
- Click Create
- A JSON file with download to your browser. That JSON file contains a client ID and a secret key. Please send the file to me in a secure fashion

#### Enable Google Sheet API
- Go back to Google Cloud Console (console.cloud.google.com)
- Search for "Google Sheets API"
- Click Manage
- At top, click on Enable API

#### Create a New Google Sheet
- Under the appropriate account, go to sheets.google.com and create a new Google Sheet for the data
- Give the Google Sheet a name, such as "VOC Performance Data"
- Click Share button in upper-right
- Paste in the Account ID (email address of the service account) from the earlier step
- Role should be editor
- Click Send

#### Optionally Grant Editor Access

Editor access to this single Google Sheet will allow someone to set up formatting and peform other maintenance as needed.

- Click Share, add the email address of the new editor
- Select Editor 
- Click Send or Done