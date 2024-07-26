import os
import logging
import argparse
from scripts.generate_data import GenerateData
from scripts.load_data import LoadData
from scripts.read_data import ReadData

class Runner:

    def __init__(self, type_exc='', trx_count = '', bucket_id = '', gcp_secrets='', data_csv_path='', parquet_path='', parquet_split_folder='', parquet_split_path =''):
        self.type_exc = type_exc
        self.trx_count = trx_count
        self.bucket_id = bucket_id
        self.gcp_secrets = gcp_secrets
        self.data_csv_path = data_csv_path
        self.parquet_path = parquet_path
        self.parquet_split_folder = parquet_split_folder
        self.parquet_split_path=parquet_split_path

    def run(self):
        logging.basicConfig(format='%(levelname)s:  %(message)s', level=logging.INFO)
        
        if self.type_exc == "all":
           self.generate()
           self.load()
           self.read()
        elif self.type_exc == "generate":
           self.generate()
        elif self.type_exc == "load":
            self.load()
        elif self.type_exc == "read":
            self.read()
        else:
            print("Type Error")
        
        return 
    def generate(self):
        generator = GenerateData(
            trx_count=self.trx_count,
            data_csv_path= self.data_csv_path,
        )
        generator.run()
        generator.csv_export()
        return 

    def load(self):
        loader = LoadData(
            data_csv_path = self.data_csv_path,
            parquet_path = self.parquet_path,
            parquet_split_folder = self.parquet_split_folder,
            parquet_split_path = self.parquet_split_path,
            bucket_id = self.bucket_id,
            gcp_secrets = self.gcp_secrets
        )
        loader.csvToParquet()
        loader.loadParquetsToGCP()
        return
    
    def read(self):
        reader = ReadData(
            bucket_id = self.bucket_id,
            gcp_secrets = self.gcp_secrets
        )
        reader.get_files_from_gcp()
        return
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='inputs')
    parser.add_argument('--type', type=str, required=True, help='Tipo de ejecucion: all, generate, load, read')
    parser.add_argument('--count', type=str, required=False, help='Cantidad de registros a generar, no obligatorio')
    args = parser.parse_args()
    
    type_exc = "all"
    if args.type is not None:
        type_exc = args.type
    
    trx_count = 0
    if args.count is not None and args.count.isnumeric():
        trx_count = int(args.count)
    
    main = Runner(
        type_exc = type_exc,
        trx_count = trx_count,
        data_csv_path = os.getenv('DATA_CSV_PATH'),
        parquet_path = os.getenv('PARQUET_PATH'),
        parquet_split_folder = os.getenv('PARQUET_SPLIT_FOLDER'),
        parquet_split_path = os.getenv('PARQUET_SPLIT_PATH'),
        bucket_id = os.getenv('BUCKET_ID'),
        gcp_secrets = os.getenv('GCP_SECRETS')
    )

    main.run()