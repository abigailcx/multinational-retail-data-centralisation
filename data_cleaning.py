from dateutil import parser
import datetime
import pandas as pd
import sqlalchemy
import re
from tqdm import tqdm

class DataCleaner:
    """
    Includes methods to clean data from each of the data sources
    """
    def __init__(self, user_table=None, card_table=None, store_table=None, products_table=None) -> None:
        self.users_rds_table = user_table
        self.card_info_table = card_table
        self.store_info_table = store_table
        self.products_info_table = products_table


    @staticmethod
    def remove_null_values(dataframe):
        print("Removing null values...")

        dataframe.replace('NULL', None, inplace=True)
        dataframe.dropna(inplace=True)


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
            dataframe.dropna(inplace=True)
 

    @staticmethod
    def normalise_month_year_dates(dataframe, columns_to_normalise):
        """

        """
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
        Infers code "GGB" is actually intended to be "GB" (cross-referenced with country column)
        """
        print("Normalising country codes...")
        replacements_dict = {'GGB': 'GB'}
        for existing, replacement in replacements_dict.items():
            dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace(existing, replacement, regex=False)
            mask = dataframe[column_to_normalise].str.fullmatch(r'[A-Z]{2}')
            dataframe = dataframe.loc[mask]


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
    def convert_product_weights(dataframe, column_to_normalise):
        """
        replace ml with g
        if cell ends in number then g,
        strip g from those cells, turn string into number and then divide by 1000
        
        now everything should be in kg, except for bad columns
        strip kg from cells

        check each cell is numbers only, remove those that aren't
        
        
        """
        print("Converting product weights...")
        dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace('ml', 'g', regex=False)
        dataframe = dataframe[~dataframe[column_to_normalise].str.contains('x', regex=False)]
        dataframe = dataframe[~dataframe[column_to_normalise].str.contains(r'[A-Z0-9]{8}', regex=True)]
    
        
        
        grams_mask = dataframe[column_to_normalise].str.contains(r'[0-9]+g$')
        
        dataframe[column_to_normalise].where(~grams_mask, other=dataframe[column_to_normalise].str.replace('g', '', regex=False), inplace=True)
        
        # for idx, row in dataframe.iterrows():
        #     if re.match(r'[0-9]$', row[column_to_normalise]):
 
        #         row[column_to_normalise] = float(row[column_to_normalise])
        #         print(row[column_to_normalise])
        # print(dataframe[column_to_normalise].head(100))

        
        # dataframe[column_to_normalise] = pd.to_numeric(dataframe[column_to_normalise], errors='ignore')
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].astype(float, errors='ignore')
        
        
        
        # num_mask = dataframe[column_to_normalise].str.match(r'^[0-9]+$')
        # dataframe[column_to_normalise].where(~num_mask, other=dataframe[column_to_normalise].astype(str).str[0:] + '.' + dataframe[column_to_normalise].astype(str).str[:-3], inplace=True)
        
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace('kg', '000', regex=False)
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace('.', '', regex=False)
        # dataframe[column_to_normalise].where(~num_mask, other=pd.to_numeric(dataframe[column_to_normalise]), inplace=True)
        # dataframe[column_to_normalise].where(~dataframe[column_to_normalise].str.contains(r'[0-9]$', regex=True), other=pd.to_numeric(dataframe[column_to_normalise], errors='raise')/1000 , inplace=True)

        #dataframe[column_to_normalise] = dataframe[column_to_normalise].where(dataframe[column_to_normalise].str.contains(r'[0-9]g$', regex=True), dataframe[column_to_normalise].str.replace('g', '', regex=False), inplace=True)
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].where(dataframe[column_to_normalise].str.contains(r'[0-9]$', regex=True), dataframe.to_numeric(dataframe[column_to_normalise], errors='coerce') , inplace=True)
        
        # dataframe[column_to_normalise] = dataframe[column_to_normalise].str.replace('kg', '', regex=False)
        

        # dataframe[column_to_normalise] = pd.to_numeric(dataframe[column_to_normalise], errors='ignore')
        print(set(dataframe[column_to_normalise]))
        
        return dataframe


    def clean_user_data(self):
        """
        which will perform the cleaning of the user data.

        You will need clean the user data, 
        look out for NULL values, 
        errors with dates, 
        incorrectly typed values and 
        rows filled with the wrong information.
        """
        self.remove_null_values(self.users_rds_table)
        self.normalise_phone_numbers(self.users_rds_table, ['phone_number'])
        self.normalise_country_code(self.users_rds_table, 'country_code')
        self.normalise_email_addresses(self.users_rds_table, ['email_address'])
        self.normalise_dates(self.users_rds_table, ['date_of_birth', 'join_date'])

        return self.users_rds_table
  

    def clean_card_data(self):
        """
        clean the data to remove any erroneous values, NULL values or errors with formatting.
        """
        self.remove_null_values(self.card_info_table)
        self.normalise_dates(self.card_info_table, ['date_payment_confirmed'])
        self.normalise_month_year_dates(self.card_info_table, ['expiry_date'])
        self.verify_credit_card_number(self.card_info_table, ['card_number'])

        return self.card_info_table
        

    def clean_store_data(self):
        """
        clean store data 
        returns pandas dataframe

        remove 'lat' column - set is {'2XE1OWOC23', 'NULL', 'LACCWDI0SB', None, 'UXMWDMX1LC', '13KJZ890JH', 'N/A', 'OXVE5QR07O', 'A3O5CBWAMD', 'VKA5I8H32X'}
        clear to see that all are nonsense codes or 'N/A', 'NULL' string or None type
        
        remove null values

        remove nonsense codes - do this via the country_code column because 
        can then just remove any non-2-letter strings

        opening_date: normalise dates to be in YYYY-MM-DD format

        transform in 'continent':
        'eeEurope' to 'Europe'
        'eeAmerica' to 'America'
        """
        self.store_info_table.drop('lat', axis=1, inplace=True)
        self.remove_null_values(self.store_info_table)
        self.normalise_country_code(self.store_info_table, 'country_code')
        self.normalise_dates(self.store_info_table, ['opening_date'])
        self.normalise_continent_data(self.store_info_table, 'continent')

        return self.store_info_table


    def clean_products_data(self):
        """
        
        """
   
        self.remove_null_values(self.products_info_table)
    
        self.convert_product_weights(self.products_info_table, 'weight')
        

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean_store_data()