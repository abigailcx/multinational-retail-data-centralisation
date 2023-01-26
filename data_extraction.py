from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor(DatabaseConnector):
    """
    Utility class
    includes methods that help extract data from different sources
    sources to extract from: CSV, API, S3 bucket
    """
    def __init__(self) -> None:
        super().__init__(creds_filename="db_creds.yaml")
        

    def read_rds_table(self):
        """
        extract the database table to a pandas DataFrame.
        It will take in an instance of your DatabaseConnector class and 
        the table name as an argument and 
        returns: a pandas DataFrame

        Use your list_db_tables method to get the name of the table containing user data.

        Use the read_rds_table method to extract the table containing user data and 
        return a pandas DataFrame.
        """
        
        # engine = DatabaseConnector(creds_filename="db_creds.yaml").init_db_engine()
        db_connector = DatabaseConnector(creds_filename="db_creds.yaml")
        db_connector.read_db_creds()
        engine = db_connector.init_db_engine()
        table_name = db_connector.list_db_tables()[1]
        print(table_name)
        print(engine.connect())
        users = pd.read_sql_table(table_name, engine)
        print(users.head(10))

        # tables_list = self.run()
        # print(tables_list)
        # return tables_list

datex = DataExtractor()
datex.read_rds_table()