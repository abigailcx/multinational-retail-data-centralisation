# from database_utils import DatabaseConnector
import boto3
s3 = boto3.client('s3')
import pandas as pd
import requests
import tabula
from tqdm import tqdm
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class DataExtractor:
    """
    Utility class
    includes methods that help extract data from different sources
    sources to extract from: CSV, API, S3 bucket
    """
    def __init__(self) -> None:
        self.header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.stores_dict = {}

    @staticmethod
    def read_rds_table(engine, table_name):
        """
        extract the database table to a pandas DataFrame.
        It will take in an instance of your DatabaseConnector class and 
        the table name as an argument and 
        returns: a pandas DataFrame

        Use your list_db_tables method to get the name of the table containing user data.

        Use the read_rds_table method to extract the table containing user data and 
        return a pandas DataFrame.
        """
        users_df = pd.read_sql_table(table_name, engine)
        
        return users_df


    @staticmethod
    def retrieve_pdf_data(url):
        """
        which takes in a link as an argument and returns a pandas DataFrame.

        Use the tabular-py Python package, imported with tabula to extract all pages from the pdf document at following link .
        Then return a DataFrame of the extracted data.

        https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf
        """
        card_df = pd.concat(tabula.read_pdf(url, pages='all'))
        # print(card_df.head(10))
        # print(card_df)
        return card_df


    def list_number_of_stores(self, number_of_stores_endpoint):
        """
        returns: number of stores to extract (int)
        """
        response = requests.get(number_of_stores_endpoint, headers=self.header_dict)
        number_of_stores = response.json()['number_stores']
        # print(type(number_of_stores))
        return number_of_stores

    
    def retrieve_stores_data(self, retrieve_a_store_endpoint, number_of_stores_endpoint):
        """
        extracts all the stores from the API,
        saving them in a Pandas dataframe
        """
        for store_number in tqdm(range(self.list_number_of_stores(number_of_stores_endpoint)), desc='Retrieving store data...'):
            response = requests.get(f'{retrieve_a_store_endpoint}/{store_number}', headers=self.header_dict)
            stores_data = response.json()
            
            self.stores_dict[store_number] = (stores_data)
            # print(self.stores_dict)
        stores_df_initialised = pd.DataFrame(self.stores_dict)
        stores_df = stores_df_initialised.transpose()
            
        # print(stores_df)
        return stores_df

    def extract_from_s3(self, s3_address):
        """
        uses the boto3 package to download and extract the information
        returns: a pandas dataframe

        s3 address: s3://data-handling-public/products.csv
        """
        split_url = s3_address.split('/')
        print(split_url)
        s3.download_file(f'{split_url[2]}', f'{split_url[3]}', f'local_{split_url[3]}')
        products = pd.read_csv(f'local_{split_url[3]}')
        # print(products.head(10))
        return products
        


if __name__ == "__main__":
    datex = DataExtractor()
    # datex.read_rds_table()
    # datex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    # datex.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')

    # datex.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', 
                                # 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
    datex.extract_from_s3('s3://data-handling-public/products.csv')