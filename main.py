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

    # # connect to local pgadmin db to upload cleaned pandas df to db as SQL table
    # local_dbcon = DatabaseConnector('local_creds.yaml')
    # local_dbcon.connect_to_db()

    # # upload cleaned df to local db (pgadmin)
    # local_dbcon.upload_to_db('dim_users_2', cleaned_df)

    print("END OF MAIN.")

def main_2():
    card_datex = DataExtractor()
    card_table = card_datex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    card_cleaner = DataCleaner(card_table=card_table)
    cleaned_card_df = card_cleaner.clean_card_data()

    # connect to local pgadmin db to upload cleaned pandas df to db as SQL table
    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    # upload cleaned df to local db (pgadmin)
    local_dbcon.upload_to_db('dim_card_details', cleaned_card_df)

def main_3():
    store_datex = DataExtractor()
    store_table = store_datex.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details', 
                                                    'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
                                            
    store_cleaner = DataCleaner(store_table=store_table)
    cleaned_store_df = store_cleaner.clean_store_data()
    # print(cleaned_store_df)

    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    local_dbcon.upload_to_db('dim_store_details', cleaned_store_df)

def main_4():
    products_datex = DataExtractor()
    products_table = products_datex.extract_from_s3('s3://data-handling-public/products.csv')

    products_cleaner = DataCleaner(products_table=products_table)
    cleaned_products_df = products_cleaner.clean_products_data()
    print(cleaned_products_df['weight'])

    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    local_dbcon.upload_to_db('dim_products', cleaned_products_df)


def main_5():
# steps to perform:
    # read db creds
    # connect to db (AWS)
    dbcon = DatabaseConnector('db_creds.yaml')
    dbcon.connect_to_db()

    # extract db (SQL table into pandas df)
    datex = DataExtractor()
    orders_table = datex.read_rds_table(dbcon.init_db_engine(), dbcon.list_db_tables()[2]) # selects orders_table from AWS RDS

    cleaner = DataCleaner(orders_table=orders_table)
    cleaned_orders_df = cleaner.clean_orders_table()

    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    local_dbcon.upload_to_db('orders_table', cleaned_orders_df)


def main_6():
    """
    https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
    """
    dates_datex = DataExtractor()
    dates_table = dates_datex.retrieve_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    dates_cleaner = DataCleaner(dates_table=dates_table)
    cleaned_dates_df = dates_cleaner.clean_dates_table()

    local_dbcon = DatabaseConnector('local_creds.yaml')
    local_dbcon.connect_to_db()

    local_dbcon.upload_to_db('dim_date_times', cleaned_dates_df)


if __name__ == "__main__":
    # main()
    # main_2()
    # main_3()
    main_4()
    # main_5()
    # main_6()