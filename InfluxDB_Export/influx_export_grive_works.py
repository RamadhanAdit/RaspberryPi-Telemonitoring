from influxdb_client import InfluxDBClient
import pandas as pd
from datetime import timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os

# --- KONFIGURASI INFLUXDB ---
url     = "http://localhost:8086"
token   = "fM-opVTUNZ0FARtHGp5vojc6q2xjddNGgnxnTzMIJ6YOgXAME6i1Wn4LSWVgO1Hx70Vl-UicZu2Am4XsGPgQQA=="
org     = "TEKLIS PNM"
bucket  = "TemperatureSensor"

# --- KONFIGURASI GOOGLE DRIVE ---
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CLIENT_SECRET_FILE = '/home/pi/Documents/Credentials/client_secret_422444898104-5biomledsotgfbsldpimukvkkaveatgl.apps.googleusercontent.com.json'
TOKEN_FILE = '/home/pi/Documents/Credentials/token.pickle'
FOLDER_ID = '1cShAstHWd5sdE6lCjBYCefXTPUNRmOwm'  # ganti dengan folder kamu

# -- AUTENTIKASI GOOGLE DRIVE ---
creds = None
if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, 'rb') as token_file:
        creds = pickle.load(token_file)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
    with open(TOKEN_FILE, 'wb') as token_file:
        pickle.dump(creds, token_file)

service = build('drive', 'v3', credentials=creds)

# --- QUERY INFLUXDB ---
query = f'''
from(bucket : "TemperatureSensor")
    |> range(start: -1d)
    |> filter(fn : (r) => r["_measurement"] == "Transformers")
 	|> yield(name: "original_time")
'''

# --- EKPORT DATA DARI INFLUXDB ---
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()
tables = query_api.query_data_frame(org=org, query=query)

if tables.empty:
    print("Tidak ada data ditemukan")
    exit()
else:
    # --- Gabungkan semua tabel hasil query ---
    df = tables
    
    # --- KONVERSI KE WIB ---
    for col in df.columns:
        if "_time" in col or "_start" in col or "_stop" in col:
            try:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                df[col] = df[col].dt.tz_convert("Asia/Jakarta")
                df[col] = df[col].dt.tz_localize(None)
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S GMT+7")
            except Exception:
                pass  # biarkan kolom lain tetap aman
            
    # --- SIMPAN KE DALAM BENTUK FILE EXCEL ---
    # Pastikan folder penyimpanan ada, jika tidak buat foldernya
    save_dir = "/home/pi/Documents/InfluxDB_Exports/"
    os.makedirs(save_dir, exist_ok=True)
    # Membuat nama file dengan timestamp
    output_file = os.path.join(save_dir, f"influx_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    #Simpan kedalam file excel
    df.to_excel(output_file, index=False)
    print(f"Data berhasil diekspor ke: {output_file}")
    print(f"Nama file: {output_file}")
    
    # --- UPLOAD KE GOOGLE DRIVE ---
    file_metadata = {
        'name': output_file,
        'parents': [FOLDER_ID]
    }
    media = MediaFileUpload(
        output_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    uploaded_file = service.files().create(
        body=file_metadata, media_body=media, fields='id'
    ).execute()
    print("File berhasil diunggah ke Google Drive dengan ID:", uploaded_file.get('id'))