from data_extraction import DataExtractor
import pandas
import sqlalchemy
import re

class DataCleaning(DataExtractor):
    """
    Includes methods to clean data from each of the data sources
    """
    def __init__(self) -> None:
        self.data_extractor = DataExtractor()
        self.users_rds_table = self.data_extractor.read_rds_table()


    def remove_null_values(self):
        # col_names = list(self.users_rds_table.columns)
        # print(col_names)
        # for col_name in col_names:
            # print(type(self.users_rds_table[col_name]))
            # self.users_rds_table = self.users_rds_table[self.users_rds_table[f"{col_name}"].str.contains("NULL") == True]
        
        #  df = df[~df.isin(['test', 'tes']).any(axis=1)]
               # df = df[~df['your column'].isin(['list of strings'])]
        self.users_rds_table = self.users_rds_table[~self.users_rds_table.isin(["NULL"]).any(axis=1)]
        
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
        for row in self.users_rds_table.iterrows():
            row["phone_number"] = self.normalise_phone_numbers(row["phone_number"])
            # print(row["phone_number"])
        print(self.users_rds_table["phone_number"])



    def verify_dates(self):
        pass

    def verify_country_code(self):
        pass

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
        # self.normalise_phone_numbers()
        self.get_normalised_phone_numbers()
        # users_rds_table_null_removed = users_rds_table.dropna(how="any")
        # if users_rds_table.duplicated(subset=["user_uuid"], keep="first").sum() > 0:
        #     # delete duplicates
        #     print(users_rds_table.loc[users_rds_table.duplicated(subset=["user_uuid"], keep="first"), : ])
        # else:
        #     pass
        # print(users_rds_table.duplicated(subset=["user_uuid"], keep="first").sum())
        # print(users_rds_table.duplicated(subset=["first_name", "last_name", "date_of_birth"], keep="first").sum())

if __name__ == "__main__":
    cleaner = DataCleaning()
    cleaner.clean_user_data()