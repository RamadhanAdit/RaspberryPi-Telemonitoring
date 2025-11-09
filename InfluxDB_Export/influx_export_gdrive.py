from influxdb_client import InfluxDBClient
import pandas as pd
from datetime import timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- KONFIGURASI INFLUXDB ---
url     = "http://localhost:8086"
token   = "fM-opVTUNZ0FARtHGp5vojc6q2xjddNGgnxnTzMIJ6YOgXAME6i1Wn4LSWVgO1Hx70Vl-UicZu2Am4XsGPgQQA=="
org     = "TEKLIS PNM"
bucket  = "TemperatureSensor"

# --- KONFIGURASI GOOGLE DRIVE ---
SERVICE_ACCOUNT_FILE = 'Drive_Upload_JSON/influxdb-export-upload-aad9bb1590f0.json'
FOLDER_ID = 'InfluxDB Upload'

SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=credentials)

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
    # Gabungkan semua tabel hasil query
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
            
    # --- SIMPAN KE EXCEL ---
    output_file = f"influx_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Data berhasil diekspor ke home/pi/")
    print(f"Nama file: {output_file}")
    
    # --- UPLOAD KE GOOGLE DRIVE ---
    file_metadata = {
        'name': output_file,
        'parents': [FOLDER_ID]
    }
    media = MediaFileUpload(output_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"File berhasil diunggah ke Google Drive dengan ID: {file.get('id')}")