import numpy as np
import pandas as pd

class DataCleaner:
    """
    Includes methods to clean data from each of the data sources
    """
    def __init__(self, user_table=None, card_table=None, store_table=None, products_table=None, orders_table=None, dates_table=None) -> None:
        self.users_rds_table = user_table
        self.card_info_table = card_table
        self.store_info_table = store_table
        self.products_info_table = products_table
        self.orders_info_table = orders_table
        self.dates_info_table = dates_table


    @staticmethod
    def remove_null_values(dataframe):
        print("Removing null values...")

        dataframe.replace('NULL', None, inplace=True)
        dataframe.replace('', None, inplace=True)
        dataframe.dropna(inplace=True, how='all')


    @staticmethod
    def remove_nonsense_codes(df):
        print("Removing nonsense codes...")
        # df = df[~df[column_to_normalise].str.contains(r'[A-Z0-9]{8,10}', regex=True)]
        df.replace(r'^[A-Z0-9]{8,10}$', None, regex=True, inplace=True)
        df.dropna(inplace=True, how='all')

    @staticmethod
    def normalise_phone_numbers(dataframe, columns_to_normalise):
        print("Normalising phone numbers...")
        replacements_dictionary = {'\(0\)': '', '[\)\(\.\- ]' : '', '^\+': '00'}
        # this nested for loop is only a short iteration
        for col_name in columns_to_normalise:
            for existing, replacement in replacements_dictionary.items():
                dataframe[col_name] = dataframe[col_name].str.replace(existing, replacement, regex=True)
                # print(dataframe[col_name])


    @staticmethod
    def normalise_dates(dataframe, columns_to_normalise):
        """
        Normalises dates by transforming them into a uniform format YYYY-MM-DD
        issues with date of birth are:
        cell is empty
        / are used instead of - , e.g. 1944/11/30
        dates written with month as word (varying y-m-d order), e.g. 2005 January 27, July 1961 14
        """
        # iterates over list of columns (max is currently 2), so not too much iteration
        print("Normalising dates...")
        for col_name in columns_to_normalise:
            # normalise dates to same format YYYY-MM-DD and NaN for any that can't be converted
            dataframe[col_name] = pd.to_datetime(dataframe[col_name], format='%Y-%m-%d', errors='coerce')
            # drop rows that include NaNs, editing the original dataframe (returns None)
            # dataframe.dropna(inplace=True, how='all')
 

    @staticmethod
    def normalise_month_year_dates(dataframe, columns_to_normalise):
        # iterates over list of columns (max is currently 2), so not too much iteration
        print("Normalising card expiry dates...")
        for col_name in columns_to_normalise:
            # filter out any rows that do not fit the regex pattern
            dataframe = dataframe[dataframe[col_name].str.contains(r'\d{2}\/\d{2}', regex=True)]
            # boolean masking to get boolean array of which dates are valid
            mask_index = pd.to_datetime(dataframe[col_name], format='%m/%y', errors='coerce').notna().index
            dataframe = dataframe.iloc[mask_index]


    @staticmethod
    def normalise_email_addresses(dataframe, columns_to_normalise):
        """
        Normalises email address by verifying that they have the correct syntax. If they contain multiple @ symbols, this is corrected.
        All invalid email addresses include 2 sequential @ symbols
        check if re.fullmatch is true
        if not, str.replace @{2} with single @ 
        then test again. if still not fullmatch then drop 
        """
        print("Normalising email addresses...")
        for col_name in columns_to_normalise:
            # replace '@@' with '@' as it was an obvious issue during EDA
            dataframe[col_name] = dataframe[col_name].str.replace('@{2}', '@', regex=True)
            # create a boolean series which evaluates whether each email adheres to email address format
            mask = dataframe[col_name].str.fullmatch(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')
            # keep rows where email evaluated to True
            dataframe = dataframe.loc[mask]


    @staticmethod
    def normalise_country_code(dataframe, column_to_normalise):
        """
        Normalises country code by replacing known errors with corrections and otherwise looking for a regex match of 2 capital letters
        Infers code "GGB" is actually intended to be "GB" (cross-referenced with country column)
        """
        print("Normalising country codes...")
        replacements_dict = {'GGB': 'GB'}
        for existing, replacement in replacements_dict.items():
            dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace(existing, replacement, regex=False)
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace(r'^([A-Z]{2})', replacement, regex=False)
            mask = dataframe[column_to_normalise].str.fullmatch(r'[A-Z]{2}')
            print(mask)
            # dataframe = dataframe.loc[mask]


    @staticmethod
    def verify_credit_card_number(dataframe, columns_to_normalise):
        print("Verifying card numbers...")
        for col_name in columns_to_normalise:
            dataframe[col_name] = dataframe[col_name].astype(str)
            mask = dataframe[col_name].str.fullmatch(r'[0-9]+', na=False)
            dataframe = dataframe.loc[mask]  


    @staticmethod
    def normalise_continent_data(dataframe, column_to_normalise):
        print("Normalising continents...")
        replacements_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        for existing, replacement in replacements_dict.items():
            dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace(existing, replacement, regex=False)
    
    @staticmethod
    def convert_product_weights(df, column_to_normalise):
        """
        Converts product weights by stripping out all measurement letter indicators and converting all weights to kilograms
        """
        print("Converting product weights...")
        df[column_to_normalise] = df[column_to_normalise].str.strip("., ")
        df[column_to_normalise] = df[column_to_normalise].str.replace(r'ml$|g$', '', regex=True)
        df[column_to_normalise] = df[column_to_normalise].str.replace(r'^[0-9]+oz$', '', regex=True)
        df[column_to_normalise] = df[column_to_normalise].str.replace(r'^[0-9]+\sx\s[0-9]+$', '', regex=True)
        df.replace('', np.nan, inplace=True)
        df.dropna(inplace=True, how='all')
        df[column_to_normalise] = df[column_to_normalise].apply(lambda x: float(x)/1000 if 'k' not in x else x)
        df[column_to_normalise] = df[column_to_normalise].astype(str)
        df[column_to_normalise] = df[column_to_normalise].str.replace('k', '', regex=False)
        
        # df[column_to_normalise] = df[column_to_normalise].astype(float)
        # print(df[column_to_normalise].head(50))
        
        return df


    @staticmethod
    def join_columns(df, columns_to_join, new_column_name):
        df[new_column_name] = df[columns_to_join].apply(lambda row: '-'.join(row.values.astype(str)), axis=1)


    @staticmethod
    def verify_integers(df, column):
        df[column] = pd.to_numeric(df[column], downcast='integer', errors="coerce")
        df.dropna(inplace=True, how='all')
        print(set(df[column]))

        return df


    def clean_user_data(self):
        """
        Cleans user data
        Removes null values
        Normalises phone numbers, country codes, email addresses and dates
        """
        self.remove_null_values(self.users_rds_table)
        self.normalise_phone_numbers(self.users_rds_table, ['phone_number'])
        self.normalise_country_code(self.users_rds_table, 'country_code')
        self.normalise_email_addresses(self.users_rds_table, ['email_address'])
        self.normalise_dates(self.users_rds_table, ['date_of_birth', 'join_date'])

        return self.users_rds_table
  

    def clean_card_data(self):
        """
        cleans the data to remove any erroneous values, NULL values or errors with formatting.
        """
        self.remove_null_values(self.card_info_table)
        self.normalise_dates(self.card_info_table, ['date_payment_confirmed'])
        self.normalise_month_year_dates(self.card_info_table, ['expiry_date'])
        self.verify_credit_card_number(self.card_info_table, ['card_number'])

        return self.card_info_table
        

    def clean_store_data(self):
        """
        Cleans store data 
        Returns pandas dataframe

        Removes 'lat' column - set is {'2XE1OWOC23', 'NULL', 'LACCWDI0SB', None, 'UXMWDMX1LC', '13KJZ890JH', 'N/A', 'OXVE5QR07O', 'A3O5CBWAMD', 'VKA5I8H32X'}
        All are nonsense codes or 'N/A', 'NULL' string or None type
        Removes null values
        Removes nonsense codes 
        Normalises country codes to be 2 capital letter codes
        Normalises dates to be in YYYY-MM-DD format
        Verifies staff numbers
        Normalises continent data': 'eeEurope' to 'Europe', 'eeAmerica' to 'America'
        """
        self.store_info_table.drop('lat', axis=1, inplace=True)
        self.remove_null_values(self.store_info_table)
        self.remove_nonsense_codes(self.store_info_table)
        self.normalise_country_code(self.store_info_table, 'country_code')
        self.normalise_dates(self.store_info_table, ['opening_date'])
        self.normalise_continent_data(self.store_info_table, 'continent')
        self.verify_integers(self.store_info_table, 'staff_numbers')
        self.remove_null_values(self.store_info_table)
        print(self.store_info_table.shape[0])
        print(set(self.store_info_table['country_code']))
        # print(set(self.store_info_table['store_code']))

        return self.store_info_table


    def clean_products_data(self):
        """
        Cleans products data by removing null values and nonsense codes
        Converts product weights and normalises dates by calling static methods in data_cleaning.py
        """
        self.remove_null_values(self.products_info_table)
        self.remove_nonsense_codes(self.products_info_table)
        self.convert_product_weights(self.products_info_table, 'weight')
        self.normalise_dates(self.products_info_table, ['date_added'])

        return self.products_info_table


    def clean_orders_table(self):
        """
        Removes columns: first_name, last_name and 1 to have table in correct form before uploading to the database
        Orders data contains column headers which are the same in other tables
        Table will act as source of truth for sales data and will be at centre of star-based database schema
        """
        self.orders_info_table.drop(['first_name', 'last_name', '1', 'level_0'], axis=1, inplace=True)
        self.remove_null_values(self.orders_info_table) # good check to do, but no null data in this instance
        self.verify_credit_card_number(self.orders_info_table, ['card_number']) # good check to do, but no null data in this instance

        return self.orders_info_table


    def clean_dates_table(self):
        self.remove_null_values(self.dates_info_table)
        self.remove_nonsense_codes(self.dates_info_table)
        # self.join_columns(self.dates_info_table, ['year', 'month', 'day'], 'date')
        # self.normalise_dates(self.dates_info_table, ['date'])
        # self.dates_info_table.drop(['year', 'month', 'day'], axis=1, inplace=True)
        
        return self.dates_info_table


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean_store_data()