# SAMA ETL Documentation

## Overview

This Python script facilitates an EL (Extract, Transform, Load) process. It's designed to Read data from an Excel file which downloaded from website, transform it, and then load it into a destination database. The script handles various tasks such as establishing database connections, data extraction, data transformation, logging and load data.

## Usage

To use this script:

1. Install necessary Python libraries: `pandas`, `datetime`, `sqlalchemy`,`time` etc.
2. Configure database connections and settings in `ETL_Config`.
3. Run the script as a standalone Python application or in SSIS.

# Import Section

## Overview

This section of the script is dedicated to importing necessary libraries and custom modules required for the ETL process. Each import serves a specific purpose in the script, whether it's for data manipulation, database connection, or other utilities.

## Details of Imports

### Standard Libraries and Packages

- `pandas`: A powerful data manipulation and analysis tool, used extensively for data processing tasks in the script.

    ```python
    import pandas as pd
    ```

- `datetime`: Utilized for handling date and time information, crucial in time-sensitive data operations.

    ```python
    from datetime import datetime
    ```

- `time`: Provides time-related functions, useful for handling delays or time calculations.

    ```python
    import time
    ```

- `re`: Library in Python provides support for working with regular expressions, which are powerful tools for matching patterns in strings. Regular expressions are often used for tasks like searching, replacing, and splitting strings based on specific patterns.

    ```python
    import re
    ```

- `logging`: Used for logging information and errors, aiding in debugging and tracking the script's execution.

    ```python
    import logging
    ```

- `sqlalchemy`: A SQL toolkit and Object-Relational Mapping (ORM) library for Python, used for database interactions.

    ```python
    import sqlalchemy
    ```

- `SQLAlchemyError`: It is an exception class in the SQLAlchemy library that serves as a base class for all exceptions raised by SQLAlchemy during database operations. It is part of the SQLAlchemy's error handling mechanism and is designed to catch and handle errors related to database interactions.

    ```python
    from sqlalchemy.exc import SQLAlchemyError
    ```

- `OS`: library in Python provides a way to interact with the operating system in a platform-independent manner. like get current working directory

    ```python
    import os
    ```

- `shutil`: library in Python provides a collection of high-level operations on files and directories, making it easier to perform tasks like copying, moving, and removing files and directories. It builds on the lower-level operations provided by the os library, offering a more user-friendly interface for common file system tasks.

    ```python
    import shutil
    ```

- `glob`: ibrary in Python is used for finding files and directories that match a specified pattern

    ```python
    import glob
    ```


### Custom Modules

- `ETL_Config as c`: A custom module likely containing configuration settings for the ETL process.

    ```python
    import ETL_Config as c
    ```

- `ETL_com_functions as e`: Another custom module, presumably containing common functions used in the ETL operations.

    ```python
    import ETL_com_functions as e
    ```

# Global Variables

## Global Variables Initialization

### Overview

The script initializes several global variables used for database connections and configurations. These variables are set to `None` initially and are configured during the script's execution.

### Variables

- `Engine_DMDQ`: Intended for the database engine of the DM_Quality database.
- `Engine`: Database engine for the primary destination database.
- `SchemaName`: Name of the database schema in use.
- `database_name`: Name of the destination database.
- `num_src`: A variable potentially used to track the number of sources or source-related parameters.
- `start_time`: A variable potentially used to get the start time of ETL process


### Code Snippet

```python
Engine_DMDQ, Engine, SchemaName, database_name, num_src = None, None, None, None, None
```

# get_database_config Function

## Purpose

The `get_database_config` function is designed to retrieve specific database configuration settings from the ETL configuration module. This function is a key component in the script, allowing dynamic access to various database configurations based on a given key.

## Parameters

- `config_key` (str): A string key that identifies which database configuration to retrieve. This key corresponds to a specific set of configuration details in the ETL configuration module.

## Functionality

- The function accesses the `config` dictionary within the `ETL_Config` (aliased as `c`) module.
- It then retrieves the configuration details for the database associated with the provided `config_key`.
- The configuration details are expected to be stored under the `"servers"` key in the `config` dictionary of the `ETL_Config` module.

## Code Snippet

```python
def get_database_config(config_key):
    """Retrieve database configuration from ETL configuration module."""
    try:
        return c.config["servers"][config_key]
    except KeyError as error:
        logging.error(f"Configuration key {config_key} not found: {error}")
        raise
```

# `establish_connections(dest_config_key, dmdq_config_key)`

## Purpose
The `establish_connections` function establishes connections to two databases based on the provided configuration keys. It retrieves and sets the schema and database names as global variables.

## Parameters
- `dest_config_key` (str): The configuration key for the destination database.
- `dmdq_config_key` (str): The configuration key for the DM_Quality database.

## Returns
- `Engine_DMDQ`: The database engine connection object for the DM_Quality database.
- `Engine`: The database engine connection object for the destination database.
- `SchemaName` (str): The schema name retrieved from the destination database configuration.
- `database_name` (str): The database name retrieved from the destination database configuration.

## Raises
- `Exception`: If there is an error while establishing the database connections, the function logs the error and raises the exception.


## Code Snippet
```python
def establish_connections(dest_config_key, dmdq_config_key):
    """
    Establishes database connections based on provided configuration keys.
    """
    global Engine_DMDQ, Engine, SchemaName, database_name
    try:
        # Establish connections to the destination and DM_Quality databases
        Engine_DMDQ, Engine = e.connect_to_databases(dest_config_key, dmdq_config_key)
        # Retrieve schema and database name from configuration
        SchemaName = get_database_config(dest_config_key)["schema"]
        database_name = get_database_config(dest_config_key)["database"]
        return Engine_DMDQ, Engine, SchemaName, database_name
    except Exception as error:
        logging.error(f"Error establishing database connections: {error}")
        raise

```

# `rename_30d`

## Purpose
The `rename_30d` function takes a row of titles, typically from a DataFrame (excluding the first column), and generates new column names formatted for consistency and clarity. The column names are generated based on predefined patterns, such as `Number_of_Transactions` and `Sales`, followed by the cleaned-up title.

## Args
- `row`: A pandas Series representing a row of titles (excluding the first column).

## Returns
- `columns_30d`: A list of column names generated based on the titles in the row.

## Example:
If the row contains the title `'Restaurants & Café'`, the function will generate:
- `'Number_of_Transactions_Restaurants_and_Café'`
- `'Sales_Restaurants_and_Café'`

## Code Snippet

```python
def rename_30d(row):
    """
    Generate column names based on the provided row of titles.

    Args:
    - row: A pandas Series representing a row of titles except the first column.

    Returns:
    - columns_30d: A list of column names generated based on the titles in the row.

    Example:
        If we have a title like 'Restaurants & Café', it will generate:
        - 'Number_of_Transactions_Restaurants_and_Café'
        - 'Sales_Restaurants_and_Café'
    """
    # Convert the row to a list, removing NaN values
    titles = row.tolist()
    titles = [x for x in titles if str(x) != 'nan']
    
    # Define base strings for the column names
    salesCount = 'Number_of_Transactions'
    sales = 'Sales'
    
    # Initialize the list with the first column as 'Period'
    columns_30d = ['Period']
    
    # Generate formatted column names for each title
    for main_title in titles:
        main_title = main_title.strip()
        main_title = main_title.replace('*', '')
        main_title = main_title.replace("&", "and")
        main_title = main_title.replace(' ', '_')
        
        # Append formatted column names
        columns_30d.append(salesCount + "_" + main_title)
        columns_30d.append(sales + "_" + main_title)
    
    return columns_30d
```
# `rename_30e`

## Purpose
The `rename_30e` function takes a row of titles from a DataFrame (excluding the first column) and generates new column names using predefined prefixes: `Number_of_Transactions`, `Sales`, and `Number_of_Terminals`. The function cleans and formats each title to ensure the generated column names are standardized.

## Args
- `row`: A pandas Series representing a row of titles except the first column.

## Returns
- `columns_30e`: A list of column names generated based on the titles in the row.

## Example
If the row contains the title `'Riyadh'`, the function will generate:
- `'Number_of_Transactions_Riyadh'`
- `'Sales_Riyadh'`
- `'Number_of_Terminals_Riyadh'`

## Code Snippet

```python
def rename_30e(row):
    """
    Generate column names based on the provided row of titles.

    Args:
    - row: A pandas Series representing a row of titles except the first column.

    Returns:
    - columns_30e: A list of column names generated based on the titles in the row.

    Example:
        If we have a title like 'Riyadh', it will generate:
        - 'Number_of_Transactions_Riyadh'
        - 'Sales_Riyadh'
        - 'Number_of_Terminals_Riyadh'
    """
    # Convert the row to a list, removing NaN values
    titles = row.tolist()
    titles = [x for x in titles if str(x) != 'nan']
    
    # Define base strings for the column names
    salesCount = 'Number_of_Transactions'
    sales = 'Sales'
    devices = 'Number_of_Terminals'
    
    # Initialize the list with the first column as 'Period'
    columns_30e = ['Period']
    
    # Generate formatted column names for each title
    for main_title in titles:
        main_title = main_title.strip()  # Remove extra spaces
        main_title = main_title.replace('-', '')  # Remove hyphens
        
        # Append formatted column names
        columns_30e.append(salesCount + "_" + main_title)
        columns_30e.append(sales + "_" + main_title)
        columns_30e.append(devices + "_" + main_title)
    
    # Remove specific items from the list
    columns_30e = [column_name for column_name in columns_30e if str(column_name) != 'Number_of_Terminals_الفترة']
    
    return columns_30e
```
# `split_and_keep_integer`

## Purpose

The `split_and_keep_integer` function is useful when you need to extract the integer part of a floating-point number or handle missing values in a dataset.

## Args
- `value`: A numeric value that can be a float, integer, or NaN.

## Returns
- The integer part of the number as a string, or `0` if the input is NaN.

## Example
If the input value is `123.456`, the function will return `'123'`.

## Code Snippet

```python
def split_and_keep_integer(value):
    """
    Split the input value at the decimal point and return the integer part.

    Args:
    - value: A numeric value that can be a float, integer, or NaN.

    Returns:
    - The integer part of the number as a string, or 0 if the input is NaN.
    
    Example:
        If the value is 123.456, it returns '123'.
        If the value is NaN, it returns 0.
    """
    if pd.isna(value):
        return 0  # Handle NaN values by converting them to 0
    return str(value).split('.')[0]
```
# `mapping_sheet_name`

## Purpose

The `mapping_sheet_name` function takes a sheet name as input and returns a corresponding part of a table name using predefined mappings stored in a dictionary. If the provided sheet name does not exist in the dictionary, the function will raise a `KeyError`.

## Args
- `original_sheet_name (str)`: The original sheet name to be mapped.

## Returns
- `part_table_name (str)`: Part of the table name that corresponds to the mapped value from `original_sheet_name`.

### Raises
- `KeyError`: If `original_sheet_name` is not found in the predefined mappings.

## Example
If the input `original_sheet_name` is `'30d'`, the function will return `'SAMA_Points_of_Sale_Transactions_by_Sectors_by'`.

## Code Snippet

```python
def mapping_sheet_name(original_sheet_name):
    """
    Map the original sheet name to a part of a table name based on predefined mappings.

    Args:
    - original_sheet_name (str): The original sheet name to be mapped.

    Returns:
    - part_table_name (str): Part of the table name based on the mapped value from `original_sheet_name`.

    Raises:
    - KeyError: If `original_sheet_name` is not found in the predefined mappings.
    """
    # Specify the list of table names for each DataFrame
    table_mappings = {
        '30c': 'SAMA_POINTS_OF_SALE_AND_TRANSACTIONS_by',
        '30d': 'SAMA_Points_of_Sale_Transactions_by_Sectors_by',
        '30e': 'SAMA_Points_of_Sale_Transactions_by_Main_Cities_by',
    }
    
    # Retrieve the mapped table name part
    part_table_name = table_mappings[original_sheet_name]

    return part_table_name
```
# `move_file_to_archive(file_path)`

## purpose
The `move_file_to_archive` function moves files with names matching a specified pattern (e.g., `Monthly_Bulletin_*.xlsx`) from the current working directory to an `Archive` subdirectory. If no matching files are found, the function logs an informational message.

## Parameters
- `file_path` (str): The pattern for the file name(s) to search for within the current working directory.

## Returns
- `None`

## Raises
- `FileNotFoundError`: If the specified file does not exist.
- `PermissionError`: If there is an issue with file permissions.
- `Exception`: For any other exceptions that might occur during the file moving process.

## Logging
- Logs an informational message if no files matching the pattern are found.
- Logs an informational message when a file is successfully moved to the `Archive` directory.
- Logs an error message if a `FileNotFoundError`, `PermissionError`, or any other exception occurs.


## Code Snippet
```python
def move_file_to_archive(file_path):
    """
    Move files with names matching 'Monthly_Bulletin_*xlsx' to the 'Archive' directory.

    Parameters:
    file_path (str): The pattern file name we need to look for inside current working dir.

    Returns:
    None

    Raises:
    FileNotFoundError: If the specified file does not exist.
    PermissionError: If there is an issue with file permissions.
    Exception: For any other exceptions that might occur.
    """
    try:
        save_directory = os.getcwd()
        archive_directory = os.path.join(save_directory, 'Archive') # Join the current working directory with the subdirectory 'Archive'
        # Find all files matching the pattern
        pattern = os.path.join(save_directory, file_path)
        files = glob.glob(pattern)
        
        if not files:
            logging.info("No files matching the pattern were found.")
            return
        
        for file_path in files:
            shutil.move(file_path, archive_directory)
            logging.info(f"File moved to: {archive_directory}")

    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}. Exception: {e}")
    except PermissionError as e:
        logging.error(f"Permission error while moving file: {file_path}. Exception: {e}")
    except Exception as e:
        logging.error(f"An error occurred while moving the file: {file_path}. Exception: {e}")

```

# `read_excel_sheets`

## Purpose
The `read_excel_sheets` function is designed to read predefined sheets from multiple Excel files that match a specified pattern. It handles errors gracefully by logging them and ensures that data is read efficiently.

## Parameters
- `pattern (str)`: A string pattern used to match Excel files, such as `'*.xlsx'` to match all Excel files in a directory.

### Returns
- `list of tuples`: Each tuple contains:
  - The first element: Sheet name.
  - The second element: Corresponding DataFrame for that sheet.

## Example
If the pattern is `'data/*.xlsx'`, the function reads sheets `30c`, `30d`, and `30e` from all matching Excel files.

## Code Snippet
```python
def read_excel_sheets(pattern):
    """
    Read sheets: 30c, 30d, 30e in an Excel file and return them as a list of tuples.

    Parameters:
        pattern (str): Pattern to match Excel files, e.g., '*.xlsx'.

    Returns:
        list of tuples: A list where each tuple contains the sheet name and the corresponding DataFrame.
    """
    global start_time
    try:
        start_time = time.time()  # Record start time
        # Find all files matching the pattern
        files = glob.glob(pattern)
        if not files:
            print("No files matching the pattern were found.")
            return []

        # Initialize list to store the data from each sheet
        sheets_data = []
        
        # Iterate over each matching file
        for file in files:
            try:
                # Read specific sheets from the Excel file
                excel_data = pd.read_excel(file, sheet_name=['30c', '30d', '30e'], header=12)  # Read data with header at row 12
                logging.info(f"Started reading data from file: {file}")
                
                # Convert the dictionary of DataFrames to a list of tuples
                sheets_data.extend([(sheet_name, excel_data[sheet_name]) for sheet_name in excel_data])
            
            except FileNotFoundError:
                logging.error(f"File '{file}' not found.")
                continue
            except Exception as e:
                logging.error(f"An error occurred while reading Excel file '{file}': {str(e)}")
                continue
    
    except Exception as e:
        logging.error(f"An error occurred while reading Excel files: {str(e)}")
        return None
    
    try:
        logging.info("Finished reading all sheets.")
        return sheets_data
    
    except Exception as e:
        logging.error(f"An error occurred while processing Excel sheets: {str(e)}")
        return None
```
# `transform_data`

## Purpose
The `transform_data` function processes each sheet of data, extracts relevant information, applies various transformations, and organizes the data into separate DataFrames for different time intervals. These DataFrames are then stored in a dictionary with corresponding table names.

## Parameters
- `sheets_data (list)`: A list of tuples where each tuple contains:
  - The sheet name.
  - The corresponding DataFrame for that sheet.

## Returns
- `transformed_data (dict)`: A dictionary where keys are table names (`*_Year`, `*_Quarter`, `*_Month`) and values are the transformed DataFrames.

## Transformation Steps
1. **Extract Data**: Start from the row where the column `'الفترة'` is found.
2. **Drop NaN Columns**: Remove columns with all NaN values.
3. **Combine and Clean Columns**: Merge two specific columns into one, removing unnecessary values like `'nan'`.
4. **Rename Columns**: Rename columns based on the sheet type (`30c`, `30d`, `30e`).
5. **Remove Specific Columns**: Drop columns containing `'_الفترة'`.
6. **Add Timestamps**: Add a column with the current date and time.
7. **Split Period**: Split the `'Period'` column into `'Yearnum'` and `'Qurternum'` for quarterly data.
8. **Filter Rows**: Filter rows based on patterns for yearly, quarterly, and monthly data.
9. **Data Type Conversion**: Convert data types for specific columns based on content (e.g., integers for transaction counts).
10. **Store Data**: Save the transformed DataFrames into a dictionary with appropriate table names.

## Code Snippet
```python
def transform_data(sheets_data):
    """
    Transform raw data from Excel sheets into formatted DataFrames for different time periods.

    Args:
    - sheets_data (list): A list of tuples where each tuple contains a sheet name and its corresponding DataFrame.

    Returns:
    - transformed_data (dict): A dictionary where keys are table names and values are DataFrames with transformed data.
    """
    transformed_data = {}

    for sheet_name, df in sheets_data:
        part_table_name = mapping_sheet_name(sheet_name)
        try:
            logging.info(f"Transforming SAMA data: {sheet_name}...")
            
            # Extract data starting from the row where 'الفترة' is found
            start_index = df.index[df['Unnamed: 1'] == 'الفترة'][0]
            df = df.iloc[start_index:]
            
            # Drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            
            # Combine data from two columns into one and delete the second column
            df["Unnamed: 1"] = df["Unnamed: 1"].astype(str) + ' ' + df["Unnamed: 2"].astype(str)
            df['Unnamed: 1'] = df['Unnamed: 1'].str.replace("nan", "")
            del df["Unnamed: 2"]

            # Extract first row and all columns except the first one
            subset_data = df.iloc[0, 1:]

            # Rename columns based on the sheet name
            if sheet_name == '30c':
                df.rename(columns=columns_30c, inplace=True)
                df.replace('---', np.nan, inplace=True)
            elif sheet_name == '30d':
                renamed_30d_columns = rename_30d(subset_data)
                df.columns = renamed_30d_columns
            elif sheet_name == '30e':
                renamed_30e_columns = rename_30e(subset_data)
                df.columns = renamed_30e_columns

            # Drop columns containing '_الفترة' in the name
            columns_to_drop = [col for col in df.columns if '_الفترة' in col]
            df.drop(columns=columns_to_drop, inplace=True)
        
            # Add 'STG_CreatedDate' column with the current datetime
            df['STG_CreatedDate'] = datetime.now()
            # Replace NaN values with '0'
            df.replace(np.nan, '0.0', inplace=True)

            # Identify the start and last index where 'Period' column contains 'Q'
            first_Q_index = df[df['Period'].str.contains('Q1')].index.tolist()[0]
            last_Q_index = df[df['Period'].str.contains('Q1')].index.tolist()[-1]

            # Define patterns to match datetime formats
            year_pattern = r'^\d{4}|^\d{4}-12-31 00:00:00'
            month_pattern = r'^\d{4}-\d{2}-\d{2} 00:00:00'

            # Filter rows for yearly data
            year_df = df.loc[:first_Q_index - 1]
            year_df = year_df[year_df['Period'].astype(str).str.match(year_pattern)]

            # Convert specific columns to integer and float types
            year_number_columns = [col for col in year_df.columns if 'Number' in col]
            for col in year_number_columns:
                year_df[col] = year_df[col].apply(split_and_keep_integer).astype('Int64')

            year_sales_columns = [col for col in year_df.columns if 'Sales' in col]
            for col in year_sales_columns:
                year_df[col] = pd.to_numeric(year_df[col], errors='coerce').astype('Float64')

            # Filter and transform quarterly data
            quarter_df = df[df['Period'].str.contains('Q')]
            quarter_df.insert(quarter_df.columns.get_loc('Period') + 1, 'Qurternum',
                              df['Period'].str.split(pat=' ', expand=True)[0], allow_duplicates=False)
            quarter_df.insert(quarter_df.columns.get_loc('Period'), 'Yearnum',
                              df['Period'].str.split(pat=' ', expand=True)[1], allow_duplicates=False)

            quarter_df['Yearnum'] = quarter_df['Yearnum'].str.extract('(\d{4})', expand=False).astype(int)
            quarter_df.drop('Period', axis=1, inplace=True)

            quarter_number_columns = [col for col in quarter_df.columns if 'Number' in col]
            for col in quarter_number_columns:
                quarter_df[col] = quarter_df[col].apply(split_and_keep_integer).astype('Int64')

            quarter_sales_columns = [col for col in quarter_df.columns if 'Sales' in col]
            for col in quarter_sales_columns:
                quarter_df[col] = pd.to_numeric(quarter_df[col], errors='coerce').astype('Float64')

            # Filter rows for monthly data
            month_df = df.loc[last_Q_index + 1:]
            month_df = month_df[month_df['Period'].astype(str).str.match(month_pattern)]
            
            month_number_columns = [col for col in month_df.columns if 'Number' in col]
            for col in month_number_columns:
                month_df[col] = month_df[col].apply(split_and_keep_integer).astype('Int64')

            month_sales_columns = [col for col in month_df.columns if 'Sales' in col]
            for col in month_sales_columns:
                month_df[col] = pd.to_numeric(month_df[col], errors='coerce').astype('Float64')

            # Define table names
            table_name_year = part_table_name + '_Year'
            table_name_quarter = part_table_name + '_Quarter'
            table_name_month = part_table_name + '_Month'

            # Store transformed DataFrames in the dictionary
            transformed_data[table_name_year] = year_df
            transformed_data[table_name_quarter] = quarter_df
            transformed_data[table_name_month] = month_df

            logging.info(f"Finished transforming SAMA data: {part_table_name}")
        
        except Exception as e:
            logging.error(f"An error occurred while transforming {sheet_name}: {str(e)}")
            continue

    return transformed_data
```
# `load_transformed_dataframes`

## Purpose

The `load_transformed_dataframes` function iterates through a dictionary of transformed DataFrames, loads them into a database, and performs the following tasks:
1. Creates a temporary table for each DataFrame.
2. Inserts new records into the destination table where they do not already exist.
3. Drops the temporary table after the insertion.
4. Logs the execution time for each table and the total time for the entire process.

### Parameters
- `transformed_dataframes (dict)`: A dictionary where:
  - Keys are table names.
  - Values are the corresponding transformed DataFrames.

- `dest_engine`: SQLAlchemy engine connected to the destination database.

- `schema_name (str)`: The schema name where the destination tables are located.

## Returns
- `total_execution_time (str)`: Total time taken (in seconds) to read data, process it, and load it into the database, formatted to two decimal places.

## Notes
- The function uses a temporary table to facilitate the insertion of new records.
- It handles both quarterly and non-quarterly tables with different SQL queries.

## Code Snippet
```python

def load_transformed_dataframes(transformed_dataframes, dest_engine, schema_name):
    """
    Load the transformed dataframes into DB tables.

    Parameters:
        transformed_dataframes (dict): A dictionary where keys are sheet names and values are corresponding transformed DataFrames.
        dest_engine : engine created on destination table
        schema_name : schema name where destination table is located

    Returns:
        total_execution_time (float): Total time in seconds from start reading data until loading to DB table
    """
    execution_times = []
    
    for table_name, df in transformed_dataframes.items():
        try:
            logging.info(f"Loading transformed data to {table_name}...")
            
            # Start the timer
            start_time = time.time()
            
            # Create a temporary table to hold the new data
            temp_table_name = f"temp_{table_name}"
            df.to_sql(temp_table_name, con=dest_engine, schema=schema_name, if_exists='replace', index=False)
            
            with dest_engine.connect() as connection:
                if 'Quarter' in table_name:
                    # Insert new records where the combination of 'Yearnum' and 'Qurternum' does not exist
                    insert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)})
                    SELECT {', '.join(df.columns)}
                    FROM {schema_name}.{temp_table_name} AS temp
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {schema_name}.{table_name} AS main
                        WHERE main.Yearnum = temp.Yearnum
                        AND main.Qurternum = temp.Qurternum
                    )
                    """
                else:
                     # Insert new records where 'Period' does not exist
                    insert_query = f"""
                    INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)})
                    SELECT {', '.join(df.columns)}
                    FROM {schema_name}.{temp_table_name} AS temp
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {schema_name}.{table_name} AS main
                        WHERE main.Period = temp.Period
                    )
                    """
                connection.execute(insert_query)
            
            # Drop the temporary table
            with dest_engine.connect() as connection:
                connection.execute(f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name}")
            
            # Calculate load time
            load_time = time.time() - start_time
            execution_times.append(load_time)
            logging.info(f"Successfully loaded transformed data to {table_name}")
            
        except Exception as e:
            logging.error(f"Error loading DataFrame into {table_name}: {e}")

    total_execution_time = sum(execution_times)
    logging.info(f"Successfully loaded Transformed data into {schema_name} database in {total_execution_time:.2f} seconds.")

    return format(total_execution_time, ".2f")
```

# `log_data_load`

## Purpose
The `log_data_load` function records data loading activities into a specified logging table in a database. It captures details such as the execution time, the number of rows and columns, and the frequency of loads. This information helps in tracking and auditing data loading operations.

## Parameters
- `engine_dmdq`: SQLAlchemy engine instance connected to the DM_Quality database, used to perform database operations.
- `db_name (str)`: The name of the destination database where the data was loaded.
- `schema_name (str)`: The schema name where the logging table resides.
- `table_names (list)`: A list of table names for which data loading details are being logged.
- `src_table (str)`: The name of the source table or file from which data was loaded.
- `execution_time (float)`: Total execution time (in seconds) for the data loading process.
- `data_frames (list)`: A list of DataFrames that were loaded into the database.

## Raises
- `Exception`: If there is an error during the logging of data load details.

## Notes
- The function uses helper methods `Generate_Frequency_of_load` and `Insert_TO_DMDQ` from an external module `e`, which are expected to handle frequency calculations and data insertion, respectively.
- The number of rejected rows is calculated based on the difference between the source and destination counts (commented out in the example).
- Logging is used to record the success or failure of the logging process.

## Code Snippet
```python
def log_data_load(engine_dmdq, db_name, schema_name, table_names, src_table, execution_time, data_frames):
    """
    Log data loading details to a database table for monitoring and auditing purposes.
    
    Parameters:
    - engine_dmdq: The SQLAlchemy engine instance for the DM_Quality database.
    - db_name: The name of the destination database.
    - schema_name: The name of the schema where the logging table resides.
    - table_names: A list of table names for which data loading is being logged.
    - src_table: The name of the source table (or file) for logging purposes.
    - execution_time: The total execution time for the data load process.
    - data_frames: The list of DataFrames that were loaded into the database.
    
    Raises:
    - Exception: If there is an error during the logging of data load details.
    """
    try:
        for table_name, data_frame in zip(table_names, data_frames):
            rows, cols = data_frame.shape
            count = e.Generate_Frequency_of_load(engine_dmdq, table_name)
            # dest_count_df = e.read_database_count(db_name, schema_name, table_name, con=Engine)
            # count_of_dest = int(dest_count_df.iloc[0, 0])
            src_type = "EXCEL"
            # rejected_rows = 0
            # num_src = sum([df.shape[0] for df in data_frames])
            rejected_rows = 0  # num_src - count_of_dest
            e.Insert_TO_DMDQ(engine_dmdq, db_name, schema_name, table_name, execution_time, cols, rows, count, datetime.now(), src_table, src_type, rejected_rows)
            logging.info(f"Data load logged successfully for {table_name}.")
    except Exception as error:
        logging.error(f"Error logging data load: {error}")
        raise

```
# `check_for_xlsx_files()`

## Purpose
The `check_for_xlsx_files` function checks if there are any Excel files (i.e., files with a `.xlsx` extension) in the current working directory.

## Returns
- **`bool`**: 
  - Returns `True` if there is at least one file with a `.xlsx` extension in the current working directory.
  - Returns `False` if no such files are found.

### Function Workflow
1. **Get the Current Working Directory**:
   - The function uses `os.getcwd()` to get the path of the current working directory.
   
2. **List Files in the Directory**:
   - It then lists all files in this directory using `os.listdir(current_directory)`.

3. **Check for `.xlsx` Files**:
   - The function iterates over the list of files and checks if any file ends with the `.xlsx` extension.
   
4. **Return the Result**:
   - If an `.xlsx` file is found, the function immediately returns `True`.
   - If the loop completes without finding an `.xlsx` file, it returns `False`.



## Code Snippet

```python
def check_for_xlsx_files():
    """
    Check if there are any files ending with .xlsx in the current working directory.

    Returns:
    bool: True if there is at least one .xlsx file, False otherwise.
    """
    current_directory = os.getcwd()
    files = os.listdir(current_directory)
    
    for file in files:
        if file.endswith('.xlsx'):
            return True
    return False
```

# `main`

The `main` function orchestrates the ETL (Extract, Transform, Load) process by coordinating the reading of Excel files, transforming data, loading it into a database, and logging the results.

## Purpose

The `main` function is the entry point for the ETL process. It performs the following steps:
1. Initializes logging and configuration keys.
2. Checks for the presence of Excel files in the current directory.
3. Establishes database connections.
4. Reads data from Excel sheets into DataFrames.
5. Transforms the raw data into the desired format.
6. Loads the transformed data into the database.
7. Logs the data load operation.
8. Archives the processed files.
9. Handles errors and logs them.

## Parameters
This function does not take any parameters.

## Workflow
1. **Logging Initialization:**
   - Logs the start of the ETL process.

2. **Configuration and File Check:**
   - Defines configuration keys for destination and DM_Quality databases.
   - Sets the file path pattern for Excel files.
   - Calls `check_for_xlsx_files()` to determine if there are new files to process.

3. **Establish Connections:**
   - Uses `establish_connections()` to set up database connections and retrieve connection parameters.

4. **Read Excel Sheets:**
   - Calls `read_excel_sheets()` to read sheets from the Excel file and return them as a list of tuples.

5. **Transform Data:**
   - Calls `transform_data()` to transform the data and return it as a dictionary of DataFrames.

6. **Load Data:**
   - Calls `load_transformed_dataframes()` to load the transformed DataFrames into the database.

7. **Log Data Load:**
   - Calls `log_data_load()` to record details of the data loading process in the database.

8. **Move File to Archive:**
   - Calls `move_file_to_archive()` to move the processed file to an archive directory.

9. **Error Handling:**
   - Catches and logs any exceptions that occur during the ETL process.

## Code Snippet
```python
def main():
    logging.info("Starting ETL process...")
    dest_config_key = 'ByFileDB_Extrenal_Prod'
    dmdq_config_key = 'ByDB_General_Prod' 
    file_path = "Monthly_Bulletin_*.xlsx"

    # If there is an xlsx file in the current working directory, start the ETL process
    if check_for_xlsx_files(): 
        try:
            # Establish database connections
            Engine_DMDQ, Engine, SchemaName, database_name = establish_connections(dest_config_key, dmdq_config_key) 

            # Read sheets from the Excel file and return a list of tuples (sheet_name, dataframe)
            sheets_data = read_excel_sheets(file_path)
            
            # Transform data and return a dictionary with sheet names as keys and transformed DataFrames as values
            transform_dfs = transform_data(sheets_data)
            
            # Load the transformed DataFrames into the database
            execution_time = load_transformed_dataframes(transform_dfs, Engine, SchemaName)
            
            # Log the data load operation
            log_data_load(Engine_DMDQ, database_name, SchemaName, list(transform_dfs.keys()), 'SAMA', execution_time, list(transform_dfs.values()))        
            
            logging.info(f"ETL process completed successfully in {execution_time} seconds.")
        
            # Move the file to 'Archive' after processing
            move_file_to_archive(file_path)
        except Exception as error:
            logging.error(f"An error occurred in the ETL process: {error}")
    else:
        logging.info("There are no new files to be processed")

# Check if the script is being run directly and, if so, execute the main function
if __name__ == '__main__':
    main()
```
## ETL Notes 
- **Pre-Execution Data Exploration**: Begin with a thorough review of the source data to identify preprocessing requirements and ensure the extraction process aligns with the data's intended structure and format


## Contact Information

For further assistance or inquiries, please contact the development team.
