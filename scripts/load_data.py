import os
import logging
import pandas as pd
import numpy as np
from google.cloud import storage

class LoadData:

    def __init__(self, data_csv_path='', parquet_path='', parquet_split_folder='', parquet_split_path ='', bucket_id = '', gcp_secrets=''):
        self.data_csv_path = data_csv_path
        self.parquet_path = parquet_path
        self.parquet_split_folder = parquet_split_folder
        self.parquet_split_path=parquet_split_path
        self.bucket_id = bucket_id
        self.gcp_secrets = gcp_secrets

    def csvToParquet(self):
        logging.info("CONVIRTIENDO CSV A PARQUETS...")
        df_principal = pd.read_csv(self.data_csv_path)

        if not os.path.exists(self.parquet_split_folder):
            os.makedirs(self.parquet_split_folder)
        
        for n, df in enumerate(np.array_split(df_principal, 10)):
            df.to_parquet(self.parquet_split_path.format(str(n+1)))

        return 

    def loadParquetsToGCP(self):
        logging.info("CARGANDO ARCHIVOS A GCP...")
        client = storage.Client.from_service_account_json(self.gcp_secrets)
        bucket = client.get_bucket(self.bucket_id)
        files = [f for f in os.listdir(self.parquet_split_folder) if os.path.isfile(os.path.join(self.parquet_split_folder, f))]
        for file in files:
            blob = bucket.blob(file)
            blob.upload_from_filename(self.parquet_split_folder+file)
        return


