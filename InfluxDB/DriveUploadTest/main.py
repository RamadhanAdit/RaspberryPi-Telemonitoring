from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os

# === Konfigurasi ===
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CLIENT_SECRET_FILE = '/home/pi/Downloads/client_secret_422444898104-5biomledsotgfbsldpimukvkkaveatgl.apps.googleusercontent.com.json'
TOKEN_FILE = '/home/pi/credentials/token.pickle'
FOLDER_ID = '1cShAstHWd5sdE6lCjBYCefXTPUNRmOwm'  # ganti dengan folder kamu

# === Autentikasi ===
creds = None
if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
    with open(TOKEN_FILE, 'wb') as token:
        pickle.dump(creds, token)

service = build('drive', 'v3', credentials=creds)

# === Upload file ===
output_file = 'influx_export_test.xlsx'  # file uji coba
file_metadata = {'name': output_file, 'parents': [FOLDER_ID]}
media = MediaFileUpload(output_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
print("✅ File berhasil diunggah. File ID:", uploaded_file.get('id'))
