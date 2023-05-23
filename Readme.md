## Title 
GET_LOAD_UPDATE_DATA_FromCSV.


## Description: 

This Project is used to fetch data from an API and insert it into a PostgreSQL database. It performs various operations such as creating stage, dimension, and final tables, inserting unique records, updating existing records, and managing record statuses.
 
## Requirements

- Python 3.x
- `psycopg2` library
- `requests` library
- `csv` module

## Prequsites

1. Import the required libraries:

```python```
import csv
import psycopg2
import requests

2. Create an instance of the trips_config class, providing the URL of the CSV file or API as a parameter:
   my_obj = trips_config('https://data.cityofchicago.org/resource/ukxa-jdd7.csv')

3. Execute the project the data copying and database operations.

Configuration:

The configuration for the script is stored in a PostgreSQL database. The following parameters are retrieved from the trips_config table:

csv_config_file: Path to the CSV configuration file.
1. source_table_name: Name of the source/stage table in the database.
2. target_table_name: Name of the target/dimension table in the database.
3. final_table_name: Name of the final table in the database.
4. target_alias: Alias for the target table in SQL queries.
5. source_alias: Alias for the source/stage table in SQL queries.
6. final_alias: Alias for the final table in SQL queries.
7. output_file_path: Path to the output file.
8. output_file_name: Name of the output file.

Following Configured Parameters are defined in a CSV trips_config.csv:

1. source_column_names (column names for TRIPS_STAGE_TABLE)
2. target_column_names (only column names for TRIPS_DIM_TABLE)
3. target_dtype (Data_types for TRIPS_DIM_TABLE)
4. source_dtype (Data_types for TRIPS_STAGE_TABLE, its not neccessary)
5. dim_column_values (Column_names and Values for TRIPS_DIM_TABLE)
6. final_column_values (Column_names and Values for TRIPS_FINAL_TABLE)
7. final_column_names (only column names for TRIPS_FINAL_TABLE)

## How to run:

python3 script_name.py(get_load_fromcsv_tripsConfig.py)




