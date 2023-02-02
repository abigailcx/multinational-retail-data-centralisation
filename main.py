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
    cleaner = DataCleaner(user_table=user_table)
    cleaned_df = cleaner.clean_user_data()
    
    # connect to local pgadmin db to upload cleaned pandas df to db as SQL table
    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    # upload cleaned df to local db (pgadmin)
    local_dbcon.upload_to_db('dim_users', cleaned_df)

    print("END OF MAIN.")

def main_2():
    card_datex = DataExtractor
    card_table = card_datex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    card_cleaner = DataCleaner(card_table=card_table)
    card_cleaner.clean_card_data() #cleaned_card_df = 




if __name__ == "__main__":
    # main()
    main_2()