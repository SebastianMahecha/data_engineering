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

    def generate(self):
        generator = GenerateData(
            trx_count=self.trx_count,
            data_csv_path= self.data_csv_path,
        )
        generator.run()
        generator.csv_export()

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

    def read(self):
        reader = ReadData(
            bucket_id = self.bucket_id,
            gcp_secrets = self.gcp_secrets
        )
        reader.get_files_from_gcp()
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='inputs')
    parser.add_argument('--type', type=str, required=True, help='Tipo de ejecucion: all, generate, load, read')
    parser.add_argument('--count', type=str, required=False, help='Cantidad de registros a generar, no obligatorio')
    args = parser.parse_args()
    main = Runner(
        type_exc = args.type,
        trx_count = int(args.count),
        data_csv_path = 'data/data.csv',
        parquet_path = 'data/data.parquet',
        parquet_split_folder = 'data/parquets/',
        parquet_split_path = 'data/parquets/data_{0}.parquet',
        bucket_id = 'data_engineering_bucket',
        gcp_secrets = 'config/client_secret.json'
    )

    main.run()