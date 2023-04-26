import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    """
    A class containing methods to connect to the database
    and upload data to the database
    """
    def __init__(self, creds_filename) -> None:
        self.creds_filename = creds_filename
        self.creds_dict = {}


    def read_db_creds(self):
        """
        A method to read 'db_creds.yaml' 
        returns: a dictionary of credentials
        """
        with open(self.creds_filename, 'r') as f:
            self.creds_dict = yaml.safe_load(f)
            
            return self.creds_dict


    def init_db_engine(self, database_type="postgresql", database_api="psycopg2"):
        """
        Initialises database engine
        Reads the credentials from the return of read_db_creds method and initialise and return an sqlalchemy database engine object.
        """
        url = f"{database_type}+{database_api}://{self.creds_dict['RDS_USER']}:{self.creds_dict['RDS_PASSWORD']}@{self.creds_dict['RDS_HOST']}:{self.creds_dict['RDS_PORT']}/{self.creds_dict['RDS_DATABASE']}"
        # print(url)
        engine = create_engine(url)
        engine.connect()

        return engine


    def list_db_tables(self):
        """
        Lists all the tables in the database to work out what table to extract data from.
        Returns: the name of the table containing user data.
        """
        engine = self.init_db_engine()
        inspector = inspect(engine)
        print(inspector.get_table_names()) # ['legacy_store_details', 'legacy_users', 'orders_table']
    
        return inspector.get_table_names()


    def connect_to_db(self):
        self.read_db_creds()
        self.init_db_engine()
        self.list_db_tables()


    def upload_to_db(self, table_name, dataframe):
        """
        Input: table name to use in postgres Sales_Data database, Pandas data frame to be uploaded
        """
        dataframe.to_sql(table_name, self.init_db_engine())


if __name__ == "__main__":
    dbcon = DatabaseConnector('db_creds.yaml')
    dbcon.connect_to_db()
