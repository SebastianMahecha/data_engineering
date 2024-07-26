import pandas as pd
import numpy as np
import os
from google.cloud import storage

class LoadData:

    def __init__(self, csv_path='', parquet_path='', parquet_split_folder='', parquet_split_path ='', bucket_id = '', gcp_secrets=''):
        self.csv_path = csv_path
        self.parquet_path = parquet_path
        self.parquet_split_folder = parquet_split_folder
        self.parquet_split_path=parquet_split_path
        self.bucket_id = bucket_id
        self.gcp_secrets = gcp_secrets

    def csvToParquet(self):
        df_principal = pd.read_csv(self.csv_path)

        if not os.path.exists(self.parquet_split_folder):
            os.makedirs(self.parquet_split_folder)
        
        for n, df in enumerate(np.array_split(df_principal, 10)):
            df.to_parquet(self.parquet_split_path.format(str(n+1)))

        return 

    def loadParquetsToGCP(self):
        client = storage.Client.from_service_account_json(self.gcp_secrets)
        bucket = client.get_bucket(self.bucket_id)
        files = [f for f in os.listdir(self.parquet_split_folder) if os.path.isfile(os.path.join(self.parquet_split_folder, f))]
        for file in files:
            blob = bucket.blob(file)
            blob.upload_from_filename(self.parquet_split_folder+file)
        return

loader = LoadData(
    csv_path = 'data/data.csv',
    parquet_path = 'data/data.parquet',
    parquet_split_folder = 'data/parquets/',
    parquet_split_path = 'data/parquets/data_{0}.parquet',
    bucket_id = 'data_engineering_bucket',
    gcp_secrets = 'client_secret.json'
)
loader.csvToParquet()
loader.loadParquetsToGCP()
