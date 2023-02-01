from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaner

def main():
# steps to perform:
    # read db creds
    # connect to db (AWS)
    dbcon = DatabaseConnector('db_creds.yaml')
    dbcon.connect_to_db()

    # extract db (SQL table into pandas df)
    datex = DataExtractor()
    user_table = datex.read_rds_table(dbcon.init_db_engine(), dbcon.list_db_tables()[1])

    # clean data
    cleaner = DataCleaner(user_table)
    cleaned_df = cleaner.clean_user_data()
    
    # connect to local pgadmin db to upload cleaned pandas df to db as SQL table
    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    # upload cleaned df to local db (pgadmin)
    local_dbcon.upload_to_db('dim_users', cleaned_df)

    print("END OF MAIN.")

if __name__ == "__main__":
    main()