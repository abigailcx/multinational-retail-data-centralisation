import boto3
s3 = boto3.client('s3')
import pandas as pd
import requests
import tabula
from tqdm import tqdm
import api_key

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class DataExtractor:
    """
    Utility class
    includes methods that help extract data from different sources
    sources to extract from: CSV, API, S3 bucket
    """
    def __init__(self) -> None:
        self.header_dict = api_key.API_KEY
        self.stores_dict = {}

    @staticmethod
    def read_rds_table(engine, table_name):
        """
        Input: SQLAlchemy engine and table name from list_db_tables method in database_utils.py
        Returns: a pandas DataFrame
        """
        users_df = pd.read_sql_table(table_name, engine)
        
        return users_df

    @staticmethod
    def retrieve_pdf_data(url):
        """

        Retrieves PDF data from source specified in url
        Takes in a link as an argument
        Returns: a pandas DataFrame
        """
        card_df = pd.concat(tabula.read_pdf(url, pages='all'))

        return card_df

    @staticmethod
    def retrieve_json_data(url):
        """
        Retrieves json data from url using Pandas
        Returns: a pandas DataFrame
        """
        df = pd.read_json(url)
        
        return df

    def list_number_of_stores(self, number_of_stores_endpoint):
        """
        Returns: number of stores to extract (int)
        """
        response = requests.get(number_of_stores_endpoint, headers=self.header_dict)
        number_of_stores = response.json()['number_stores']

        return number_of_stores

    def retrieve_stores_data(self, retrieve_a_store_endpoint, number_of_stores_endpoint):
        """
        Extracts all the stores from the API
        Saves them in a Pandas dataframe
        """
        for store_number in tqdm(range(self.list_number_of_stores(number_of_stores_endpoint)), desc='Retrieving store data...'):
            response = requests.get(f'{retrieve_a_store_endpoint}/{store_number}', headers=self.header_dict)
            stores_data = response.json()
            self.stores_dict[store_number] = (stores_data)
        stores_df_initialised = pd.DataFrame(self.stores_dict)
        stores_df = stores_df_initialised.transpose()
            
        return stores_df

    def extract_from_s3(self, s3_address):
        """
        Uses the boto3 package to download and extract the information
        Returns: a pandas dataframe
        """
        split_url = s3_address.split('/')
        print(split_url)
        s3.download_file(f'{split_url[2]}', f'{split_url[3]}', f'local_{split_url[3]}')
        products = pd.read_csv(f'local_{split_url[3]}')

        return products
        