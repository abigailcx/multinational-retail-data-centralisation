# Multinational Retail Data Centralisation

## Summary
This project uses an ETL (extract, transform, load) process to bring together retail data from multiple sources, including Amazon RDS, REST API, PDF, CSV and JSON files. It performs data cleaning and normalisation using Pandas before storing the transformed data in a postgreSQL database, using a star-based schema for optimising storage and access.
Finally, the database is queried using SQL to gain insights such as the regions with the highest sales, yearly revenue and the velocity of sales.


## Scripts
`data_extraction.py` - contains the DataExtractor class which includes methods to extract the data from each source

`data_cleaning.py` - contains the DataCleaner class which provides all methods to clean data from the different data sources

`database_utils.py` - contains the DatabaseConnector class which holds the methods for creating a connection to the postgreSQL database and uploading to it (note that a suitable database credentials file must be provided in yaml format)

`main.py` - imports the 3 classes from the other Python files and runs the ETL pipeline for each table to be loaded into the database

`db_creds_template.yaml` - an example file to show the format of the database credentials configuration yaml file
