from influxdb_client inport InfluxDBClient
import pandas as pd
from datetime import timedelta

# --- KONFIGURASI ---
url		= "http://localhost:8086"
token    = "fM-opVTUNZ0FARtHGp5vojc6q2xjddNGgnxnTzMIJ6YOgXAME6i1Wn4LSWVgO1Hx70Vl-UicZu2Am4XsGPgQQA=="
org		= "TEKLIS PNM"
bucket	= "TemperatureSensor"

#--- INISIALISASI CLIENT ---
client = InfluxDBClient(url=url, token=token, org=org)

# --- QUERY ---
query = f'''
from(bucket : "TemperatureSensor")
    |> range(start: -1d)
    |> filter(fn : (r) => r["_measurement"] == "Transformers")
 	|> yield(name: "original_time")
'''

# --- JALANKAN QUERY ---
query_api = client.query_api()

# --- CEK DATA ---
tables = query_api.query_data_frame(org=org, query=query)
if tables.empty:
    print("Tidak ada data ditemukan")
else:
    # Gabungkan semua tabel hasil query
    df = tables
    
    # --- DEBUG ---
    '''
    print(df.dtypes)
    print(df.head())
    '''
    
    # --- KONVERSI KE WIB ---
    '''
    tables["_time"] = pd.to_datetime(tables["_time"]).dt.tz_convert("Asia/Jakarta").dt.tz_localize(None)
    tables["_time"] = tables["_time"].dt.strftime("%Y-%m-%d %H:%M:%S") + "GMT+7"
    '''
    # Konversi kolom waktu
    # Bersihkan semua kolom datetime agar bebas dari timezone
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
    '''
    output_file = "influx_export_localtime.xlsx"
    tables.to_excel(output_file, index=False)
    '''
    output_file = f"influx_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(output_file, index=False)
    
    print(f"Data berhasil diekspor ke home/pi/")