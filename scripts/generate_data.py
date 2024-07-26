import csv
import random
import logging
import datetime
import pandas as pd
from datetime import datetime, timedelta

class GenerateData:

    def __init__(self, trx_count = 0, data_csv_path=''):
        self.trx_count = trx_count
        self.data_csv_path = data_csv_path

    def run(self):
        logging.info("GENERANDO DATA...")
        # Read the CSV file and extract categories
        with open('data/category_products.csv', mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            category_products_base = [row['category_product'] for row in reader]

        with open('data/regions.csv', mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            trx_regions_base = [row['region'] for row in reader]

        trx_ids = range(1, self.trx_count + 1)
        customers = []
        trx_dates = []
        trx_amount = []
        category_products = []
        trx_regions = []

        start_trx_date = datetime(2020, 1, 1)  
        end_trx_date = datetime(2024, 6, 30) 

        for _ in range(self.trx_count):
            customers.append(random.randint(100, 10000))
            trx_dates.append(start_trx_date + timedelta(days=random.randint(0, (end_trx_date - start_trx_date).days)))
            trx_amount.append(random.randint(10000, 1000000))
            category_products.append(random.choice(category_products_base))
            trx_regions.append(random.choice(trx_regions_base))

        self.data = pd.DataFrame({
            'trx_id': trx_ids, 
            'customer_id':customers, 
            'trx_date': trx_dates, 
            'trx_amount': trx_amount, 
            'trx_region': trx_regions, 
            'category_product': category_products  
        })

    def csv_export(self):
        self.data.to_csv(self.data_csv_path, index=False)

