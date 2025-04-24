import pandas as pd
from google.cloud import storage
import os

# Paramètres GCS
bucket_name = "darties-bucket-2025"
prefix = ""  # ou ex: "excel/"
gcs_output_prefix = "csv_clean/"  # Dossier GCS où envoyer les CSV

# Chemin vers le répertoire parent
credentials_path = os.path.join(os.path.dirname(__file__), '..', 'bq-creds.json')

# Résout les ".." en chemin absolu
credentials_path = os.path.abspath(credentials_path)
# Init client avec service account
client = storage.Client.from_service_account_json(
    credentials_path
)
bucket = client.bucket(bucket_name)

# Lister les fichiers .xlsx
blobs = list(client.list_blobs(bucket_name, prefix=prefix))
xlsx_blobs = [b for b in blobs if b.name.endswith(".xlsx")]

# Créer dossiers locaux
os.makedirs("temp_xlsx", exist_ok=True)
os.makedirs("csv_output", exist_ok=True)

for blob in xlsx_blobs:
    filename_xlsx = os.path.basename(blob.name)
    local_xlsx = os.path.join("temp_xlsx", filename_xlsx)

    # ⿡ Télécharger l'Excel si pas déjà là
    if not os.path.exists(local_xlsx):
        print(f"⬇  Téléchargement : {blob.name}")
        blob.download_to_filename(local_xlsx)
    else:
        print(f"✅  Déjà présent : {filename_xlsx}")

    # ⿢ Lire et convertir chaque onglet
    xls = pd.ExcelFile(local_xlsx, engine="openpyxl")
    base = os.path.splitext(filename_xlsx)[0]

    for sheet in xls.sheet_names:
        csv_name = f"{sheet.strip()}_{base}.csv".replace(" ", "")
        local_csv = os.path.join("csv_output", csv_name)

        # 2a⃣ Créer le CSV seulement s'il n'existe pas
        if not os.path.exists(local_csv):
            df = xls.parse(sheet)
            df.to_csv(local_csv, index=False)
            print(f"💾  Sauvé localement : {local_csv}")
        else:
            print(f"⏭  CSV existe, skip : {local_csv}")

        # 2b⃣ Uploader si nécessaire
        # (on vérifie aussi sur GCS pour éviter un second upload inutile)
        blob_csv = bucket.blob(f"{gcs_output_prefix}{csv_name}")
        if not blob_csv.exists(client):
            blob_csv.upload_from_filename(local_csv)
            print(f"📤  Upload GCS : {gcs_output_prefix}{csv_name}")
        else:
            print(f"⏭  Sur GCS déjà : {gcs_output_prefix}{csv_name}")

print("🎉  Conversion + Upload terminés.")
