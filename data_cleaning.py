from data_extraction import DataExtractor

class DataCleaning(DataExtractor):
    """
    Includes methods to clean data from each of the data sources
    """
    def __init__(self) -> None:
        self.data_extractor = DataExtractor()

    def clean_user_data(self):
        """
        which will perform the cleaning of the user data.

        You will need clean the user data, 
        look out for NULL values, 
        errors with dates, 
        incorrectly typed values and 
        rows filled with the wrong information.
        """
        users_rds_table = self.data_extractor.read_rds_table()
        print(users_rds_table[users_rds_table["first_name"].str.contains("NULL")])
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