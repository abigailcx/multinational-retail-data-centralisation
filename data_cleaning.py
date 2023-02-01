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
    def __init__(self, user_table) -> None:
        self.users_rds_table = user_table
        # self.data_extractor = DataExtractor()
        # self.users_rds_table = self.data_extractor.read_rds_table()

    def remove_null_values(self):
        # col_names = list(self.users_rds_table.columns)
        # print(col_names)
        # for col_name in col_names:
            # print(type(self.users_rds_table[col_name]))
            # self.users_rds_table = self.users_rds_table[self.users_rds_table[f"{col_name}"].str.contains("NULL") == True]
        
        #  df = df[~df.isin(['test', 'tes']).any(axis=1)]
               # df = df[~df['your column'].isin(['list of strings'])]
        print("Removing null values...")
        self.users_rds_table = self.users_rds_table[~self.users_rds_table.isin(['NULL']).any(axis=1)]
        
        return self.users_rds_table


    @staticmethod
    def normalise_phone_numbers(phone_number):
        """
        +49 Germany
        +44 UK
        sub (0) with nothing
        sub + at beginning of line for 00
        remove whitespace
        remove hyphens
        remove periods
        remove brackets
        """
        remove_bracketed_zero = re.sub(r'\(0\)', '', phone_number)
        insert_exit_code = re.sub(r'^\+', '00', remove_bracketed_zero)
        remove_whitespace = re.sub(r'[\)\(\.\- ]', '', insert_exit_code)

        # print(remove_whitespace)
        return remove_whitespace

    
    def get_normalised_phone_numbers(self):

        for idx, row in tqdm(self.users_rds_table.iterrows(), total=len(self.users_rds_table), desc="Normalising phone numbers..."):
            row['phone_number'] = self.normalise_phone_numbers(row['phone_number'])
            self.users_rds_table.loc[idx, ['phone_number']] = row['phone_number']
        
        # print(self.users_rds_table['phone_number'])

    
    def verify_dates(self):
        """
        date_of_birth
        join_date

        issues with date of birth are:
        cell is empty - do a str.replace() with NaN
        / are used instead of - , e.g. 1944/11/30 - str.replace()
        dates written with month as word (varying y-m-d order), e.g. 2005 January 27, July 1961 14
        
        first translate month words to numbers using dictionary
        then 
        """
        # print(self.users_rds_table['date_of_birth'].head(50))
        date_columns = ['date_of_birth', 'join_date']
        # replace_dict = {}
        # for col_name in date_columns:
        #     self.users_rds_table = self.users_rds_table[col_name].replace(r'^\s*$', np.nan, regex=True)
            # self.users_rds_table = self.users_rds_table[col].replace('/', '-', regex=False)
            # self.users_rds_table = self.users_rds_table[col].replace(r'@{2}', '@', regex=True)
        # dictionary = {'' } 
        # for key in dictionary.keys():
        # address = address.upper().replace(key, dictionary[key])
        
        # for 

        for col_name in date_columns:
            for idx, row in tqdm(self.users_rds_table.iterrows(), total=len(self.users_rds_table), desc="Normalising dates..."):
                try:
                    datetime.datetime.strptime(row[col_name], '%Y-%m-%d')
                    
                except ValueError as e:
                    self.users_rds_table.loc[idx, [col_name]] = row[col_name].replace(' ', 'NULL')
                    parsed_date = parser.parse(row[col_name], dayfirst=True)
                    self.users_rds_table.loc[idx, [col_name]] = parsed_date.strftime('%Y-%m-%d')
                    # print(e)
                    # row['email_address'] = re.sub(r'@{2}', '@', row['email_address'])
                    
    
    def normalise_email_addresses(self):
        """
        All invalid email addresses include 2 sequential @ symbols
        """
        email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')

        for idx, row in tqdm(self.users_rds_table.iterrows(), total=len(self.users_rds_table), desc="Normalising email adressess..."):
            if not re.fullmatch(email_regex, row['email_address']):
                row['email_address'] = re.sub(r'@{2}', '@', row['email_address'])
                self.users_rds_table.loc[idx, ['email_address']] = row['email_address']
                # print(self.users_rds_table.loc[idx, ['email_address']])
                if not re.fullmatch(email_regex, row['email_address']):
                    # print(self.users_rds_table.loc[idx, ['email_address']])
                    self.users_rds_table = self.users_rds_table.drop(idx)
                    
            else:
                pass


    def normalise_country_code(self):
        """
        Infers code "GGB" is actually intended to be "GB" (cross-referenced with country column)
        """
        print("Normalising country codes...")
        corrupted_codes = ['QREF9WLI2A', 'PG8MOC0UZI', 'FB13AKRI21', 'XPVCZE2L8B', 'QVUW9JSKY3', '44YAIDY048', 'OS2P9CMHR6', 
        'IM8MN1L9MJ', '0CU6LW3NKB', '5D74J6FPFJ', 'XKI9UXSCZ1', 'NTCGYW8LVC', 'RVRFD92E48', 'LZGTB0T5Z7', 
        'VSM4IZ4EL3']
        self.users_rds_table = self.users_rds_table.loc[self.users_rds_table['country_code'].isin(corrupted_codes)==False]
        self.users_rds_table['country_code'] = self.users_rds_table['country_code'].replace('GGB', 'GB')
 
        print(set(self.users_rds_table['country_code']))


    
    def clean_user_data(self):
        """
        which will perform the cleaning of the user data.

        You will need clean the user data, 
        look out for NULL values, 
        errors with dates, 
        incorrectly typed values and 
        rows filled with the wrong information.
        """
        self.remove_null_values()
        self.get_normalised_phone_numbers()
        self.normalise_country_code()
        self.normalise_email_addresses()
        self.verify_dates()

        return self.users_rds_table
        # users_rds_table_null_removed = users_rds_table.dropna(how="any")
        # if users_rds_table.duplicated(subset=["user_uuid"], keep="first").sum() > 0:
        #     # delete duplicates
        #     print(users_rds_table.loc[users_rds_table.duplicated(subset=["user_uuid"], keep="first"), : ])
        # else:
        #     pass
        # print(users_rds_table.duplicated(subset=["user_uuid"], keep="first").sum())
        # print(users_rds_table.duplicated(subset=["first_name", "last_name", "date_of_birth"], keep="first").sum())

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean_user_data()