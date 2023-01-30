import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Access the drive
gauth = GoogleAuth()
# gauth.LocalWebserverAuth() # This will open Chrome / Google account everytime

# Try to load saved client's info into credentials file
gauth.LoadCredentialsFile("credentials.json")
if gauth.credentials is None:
    # Authenticate if they're not there
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    # Refresh them if expired
    gauth.Refresh()
else:
    # Initialize the saved credentials
    gauth.Authorize()
# Save the current credentials to a file
gauth.SaveCredentialsFile("credentials.json")

# Call Google Drive
drive = GoogleDrive(gauth)

# A) Upload CSV file (copy of what got loaded into the database) into Google Drive
csv_file = drive.CreateFile()
csv_file.SetContentFile('./data/weather_data.csv')
csv_file.Upload()

# B) Upload the CSV's data into an existing Google Sheet 
# Google Account Services creds. & google sheets' API need to be enabled
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name('weather-data-service-account.json', scope)
# You may name the previous json file however you want - you get this from google developer console
client = gspread.authorize(credentials)
spreadsheet_id = '2Ms*************************' # obtained from google sheet's url

# Content gets copied into from CSV into created google sheet
with open('./data/weather_data.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet_id, data=content)
