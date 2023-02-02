# from database_utils import DatabaseConnector
import pandas as pd
import tabula
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class DataExtractor:
    """
    Utility class
    includes methods that help extract data from different sources
    sources to extract from: CSV, API, S3 bucket
    """
    def __init__(self) -> None:
        pass

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
        print(card_df.head(10))
        # print(card_df)
        return card_df

if __name__ == "__main__":
    datex = DataExtractor()
    # datex.read_rds_table()
    datex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')