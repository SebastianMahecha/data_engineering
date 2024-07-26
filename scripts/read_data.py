import time
import logging
import tempfile
import pandas as pd
import polars as pl
import modin.pandas as mpd
from io import BytesIO
from google.cloud import storage

class ReadData:

    def __init__(self, bucket_id = '', gcp_secrets=''):
        self.bucket_id = bucket_id
        self.gcp_secrets = gcp_secrets

    def get_files_from_gcp(self):
        logging.info("LENYENDO ARCHIVOS...")
        client = storage.Client.from_service_account_json(self.gcp_secrets)
        files = client.list_blobs(self.bucket_id, prefix='')
        i = 0

        new_rows = {
            'trx_id': [100000001, 100000002, 100000003],
            'customer_id':[8888, 9999, 7777],
            'trx_date':['2024-05-07', '2024-05-08', '2021-05-08'],
            'trx_amount':[100000, 111000, 112000],
            'trx_region':['Santander','Sucre','Tolima'],
            'category_product':['Muebles', 'Juguetes' , 'Joyería'],
        }

        parquets = []
        for file in files:
            parquets.append(file.download_as_bytes())
        
        duration_load_pandas, duration_operation_pandas = self.operation_with_pandas(parquets, new_rows)
        duration_load_polars, duration_operation_polars = self.operation_with_polars(parquets, new_rows)
        duration_load_modin, duration_operation_modin =  self.operation_with_modin(parquets, new_rows)
        
        durations_load = {
            'pandas': duration_load_pandas,
            'polars': duration_load_polars,
            'modin':  duration_load_modin
        }

        durations_operation = {
            'pandas': duration_operation_pandas,
            'polars': duration_operation_polars,
            'modin':  duration_operation_modin
        }
        
        logging.info("-------------------------")
        logging.info("Escalafon de carga")
        
        for i, lib in enumerate(sorted(durations_load.items(), key=lambda x:x[1])):
            logging.info("{0}: {1} - {2} ".format(i+1, lib[0], lib[1]))
        
        logging.info("-------------------------")
        logging.info("Escalafon de operacion")
        
        for i, lib in enumerate(sorted(durations_operation.items(), key=lambda x:x[1])):
            logging.info("{0}: {1} - {2} ".format(i+1, lib[0], lib[1]))
        
        return
    
    def operation_with_pandas(self, parquets, new_rows):
        time_start_load = time.time()
        dfs = []
        for parquet in parquets:
            dfs.append(pd.read_parquet(BytesIO(parquet)))
        
        df = pd.concat(dfs)
        time_end_load = time.time()
        duration_load = time_end_load-time_start_load
        time_start_operation = time.time()
        #Adding new records
        df_new  = pd.DataFrame(new_rows)
        df = df._append(df_new, ignore_index = True)
        df_categories = df.groupby([df['category_product']]).agg({'trx_amount': 'sum'}).sort_values(('trx_amount'),ascending=False)
        df_regions = df.groupby([df['trx_region']]).agg({'trx_amount': 'mean'}).sort_values(('trx_amount'),ascending=False)
        sum = df['trx_amount'].sum()
        mean = df['trx_amount'].mean()
        time_end_operation =  time.time()
        
        logging.info("-------------------------")
        logging.info("PANDAS")
        logging.info("Suma Total por Categoria - Pandas")
        logging.info(df_categories.to_string())
        logging.info("Promedio por Region - Pandas")
        logging.info(df_regions.to_string())
        logging.info("Suma Total Ventas - Pandas: {0}".format(sum))
        logging.info("Promedio Total Ventas - Pandas: {0}".format(mean))

        
        duration_operation = time_end_operation-time_start_operation

        return duration_load, duration_operation
    
    def operation_with_polars(self, parquets, new_rows):
        time_start_load = time.time()
        dfs = []
        for parquet in parquets:
            dfs.append(pl.read_parquet(BytesIO(parquet)))
        
        df = pl.concat(dfs)
        time_end_load = time.time()
        duration_load = time_end_load-time_start_load

        time_start_operation = time.time()
        #Adding new records
        df_new  = pl.DataFrame(new_rows)
        df = pl.concat([df, df_new])   
        df_categories = df['category_product', 'trx_amount'].group_by('category_product', maintain_order=True).sum().sort('trx_amount', descending=True)
        df_regions = df['trx_region', 'trx_amount'].group_by('trx_region', maintain_order=True).mean().sort('trx_amount', descending=True)
        sum = df['trx_amount'].sum()
        mean = df['trx_amount'].mean()
        time_end_operation =  time.time()
        
        logging.info("-------------------------")
        logging.info("POLARS")
        logging.info("Suma Total por Categoria - Polars")
        logging.info(df_categories.to_pandas().to_string())
        logging.info("Promedio por Region - Polars")
        logging.info(df_regions.to_pandas().to_string())
        logging.info("Suma Total Ventas - Polars: {0}".format(sum))
        logging.info("Promedio Total Ventas - Polars: {0}".format(mean))

        
        duration_operation = time_end_operation-time_start_operation
    
        return duration_load, duration_operation
    
    def operation_with_modin(self, parquets, new_rows):
        time_start_load = time.time()
        dfs = []
        for parquet in parquets:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_file:
                temp_file.write(parquet)
                temp_file_path = temp_file.name
            dfs.append(mpd.read_parquet(temp_file_path))
        
        df = mpd.concat(dfs)
        time_end_load = time.time()
        duration_load = time_end_load-time_start_load

        time_start_operation = time.time()
        #Adding new records
        df_new  = mpd.DataFrame(new_rows)
        df = mpd.concat([df, df_new])
        df_categories = df.groupby([df['category_product']]).agg({'trx_amount': 'sum'}).sort_values(('trx_amount'),ascending=False)
        df_regions = df.groupby([df['trx_region']]).agg({'trx_amount': 'mean'}).sort_values(('trx_amount'),ascending=False)
        sum = df['trx_amount'].sum()
        mean = df['trx_amount'].mean()
        time_end_operation =  time.time()
        
        logging.info("-------------------------")
        logging.info("MODIN")
        logging.info("Suma Total por Categoria - Modin")
        logging.info(df_categories._to_pandas().to_string())
        logging.info("Promedio por Region - Modin")
        logging.info(df_regions._to_pandas().to_string())
        logging.info("Suma Total Ventas - Modin: {0}".format(sum))
        logging.info("Promedio Total Ventas- Modin: {0}".format(mean))

        
        duration_operation = time_end_operation-time_start_operation

        return duration_load, duration_operation
    

