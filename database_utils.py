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
            # print(self.creds_dict)
            
            return self.creds_dict

    def init_db_engine(self, database_type="postgresql", database_api="psycopg2"):
        """
        will read the credentials from 
        the return of read_db_creds and 
        initialise and return an sqlalchemy database engine object.
        """
        # {'RDS_HOST': 'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com', 
        # 'RDS_PASSWORD': 'AiCore2022', 
        # 'RDS_USER': 'aicore_admin', 
        # 'RDS_DATABASE': 'postgres', 
        # 'RDS_PORT': 5432}
        url = f"{database_type}+{database_api}://{self.creds_dict['RDS_USER']}:{self.creds_dict['RDS_PASSWORD']}@{self.creds_dict['RDS_HOST']}:{self.creds_dict['RDS_PORT']}/{self.creds_dict['RDS_DATABASE']}"
        # print(url)
        engine = create_engine(url)
        engine.connect()

        return engine

    def list_db_tables(self):
        """
        list all the tables in the database so you know which tables you can extract data from.
        Use your list_db_tables method to get the name of the table containing user data.
        """
        engine = self.init_db_engine()
        inspector = inspect(engine)
        print(inspector.get_table_names()) # ['legacy_store_details', 'legacy_users', 'orders_table']
    
        return inspector.get_table_names()

        # engine.execute()

    def connect_to_db(self):
        self.read_db_creds()
        self.init_db_engine()
        self.list_db_tables()
        # self.upload_to_db()

    def upload_to_db(self, table_name, dataframe):
        """
        This method takes in a Pandas DataFrame and table name to upload to as an argument.
        
        Use this method to store the data in your Sales_Data database in a table named dim_users
        """
        dataframe.to_sql(table_name, self.init_db_engine())


if __name__ == "__main__":
    dbcon = DatabaseConnector('db_creds.yaml')
    dbcon.connect_to_db()

    #local_dbcon = DatabaseConnector('local_creds.yaml')
    # local_dbcon.run()
    #print(local_dbcon.list_db_tables())
    #test_table = DataExtractor.read_rds_table(local_dbcon.init_db_engine(), 'legacy_users' ) # local_dbcon.list_db_tables()[1]
    # dc = DataCleaner()

    #local_dbcon.upload_to_db('dim_users', test_table)