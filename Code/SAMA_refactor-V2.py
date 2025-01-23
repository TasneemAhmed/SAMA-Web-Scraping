import pandas as pd
from datetime import datetime
import time
import numpy as np
import re
import logging
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import os #to get the current working directory
import shutil # to move file to another directory
import glob #module to find all files matching the pattern

# Import custom modules
import ETL_Config as c
import ETL_com_functions as e

"""
We configure logging using basicConfig() to set the logging level to INFO. 
This means that only messages with severity level INFO and higher will be logged.
"""
logging.basicConfig(level=logging.INFO)

# Initialize global variables for database connections and configurations
Engine_DMDQ, Engine, SchemaName, database_name, num_src, start_time = None, None, None, None, None, None

def get_database_config(config_key):
    """Retrieve database configuration from ETL configuration module."""
    try:
        return c.config["servers"][config_key]
    except KeyError as error:
        logging.error(f"Configuration key {config_key} not found: {error}")
        raise

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

# Dictonary mapping to rename columns in '30c' sheet
columns_30c = {
    'Unnamed: 1': 'Period',
    'Unnamed: 3': 'Sales_Total_Points_Of_Sale_Transactions',
    'Unnamed: 4': 'Number_of_Transactions_Total_Points_Of_Sale_Transactions',
    'Unnamed: 5': 'Number_of_Points_of_Sale_Terminals',
    'Unnamed: 7': 'Number_of_Mobile_Transactions_Points_of_Sale_Transactions_Using_Near_Field_Communication_Technology',
    'Unnamed: 8': 'Number_of_Cards_Transactions_Points_of_Sale_Transactions_Using_Near_Field_Communication_Technology',
    'Unnamed: 10': 'Sales_Using_Mobile_Points_of_Sale_Transactions_Using_Near_Field_Communication_Technology',
    'Unnamed: 11': 'Sales_Using_Cards_Points_of_Sale_Transactions_Using_Near_Field_Communication_Technology',
    'Unnamed: 13': 'Sales_ECommerce_Transactions_Using_Mada_Cards',
    'Unnamed: 14': 'Number_of_Transactions_Transactions_Using_Mada_Cards'
}

def rename_30d(row):
    """
    Generate column names based on the provided row of titles.

    Args:
    - row: A pandas Series representing a row of titles except the first column.

    Returns:
    - columns_30d: A list of column names generated based on the titles in the row.

    for example:
        if we have title like : 'Restaurants & Café' >>>> 'Number_of_Transactions_Restaurants_and_Café' and 'Sales_Restaurants_and_Café'
    """
    titles = row.tolist()
    titles = [x for x in titles if str(x) != 'nan']
    
    salesCount= 'Number_of_Transactions'
    sales= 'Sales'
    
    columns_30d=['Period']
    for main_title in titles:
        main_title = main_title.strip()
        main_title = main_title.replace('*', '')
        main_title = main_title.replace("&", "and")
        main_title = main_title.replace(' ', '_')
        
        columns_30d.append(salesCount + "_" + main_title)
        columns_30d.append(sales + "_" + main_title)
    
    return columns_30d

def rename_30e(row):
    """
    Generate column names based on the provided row of titles.

    Args:
    - row: A pandas Series representing a row of titles except the first column

    Returns:
    - columns_30e: A list of column names generated based on the titles in the row.

    for example: 
        if we have title like : 'Riyadh ' >>>> 'Number_of_Transactions_Riyadh' , 'Sales_Riyadh' and 'Number_of_Terminals_Riyadh'
    """
    titles = row.tolist()
   
    titles = [x for x in titles if str(x) != 'nan']
    
    salesCount= 'Number_of_Transactions'
    sales= 'Sales'
    devices = 'Number_of_Terminals'
    
    columns_30e=['Period']
    for main_title in titles:
        main_title = main_title.strip()

        main_title = main_title.replace('-', '')
        columns_30e.append(salesCount + "_" + main_title)
        columns_30e.append(sales + "_" + main_title)
        columns_30e.append(devices + "_" + main_title)
        
    # Remove specific items from the list
    columns_30e = [column_name for column_name in columns_30e if str(column_name) != 'Number_of_Terminals_الفترة']
        
    return columns_30e

# Function to split at the decimal point and return the first part which is integer part
def split_and_keep_integer(value):
    if pd.isna(value):
        return 0  # Handle NaN values by converting them to 0
    return str(value).split('.')[0]  

def mapping_sheet_name(original_sheet_name):
    """
    Map the original sheet name to a part of a table name based on predefined mappings.

    Args:
    - original_sheet_name (str): The original sheet name to be mapped.

    Returns:
    - part_table_name (str): Part of the table name based on the mapped value from `original_sheet_name`.

    Raises:
    - KeyError: If `original_sheet_name` is not found as sheet name in excel file.

    """
    # Specify the list of table names for each DataFrame
    table_mappings = {
        '30c': 'SAMA_POINTS_OF_SALE_AND_TRANSACTIONS_by',
        '30d': 'SAMA_Points_of_Sale_Transactions_by_Sectors_by',
        '30e': 'SAMA_Points_of_Sale_Transactions_by_Main_Cities_by',
    }
    
    part_table_name = table_mappings[original_sheet_name]

    return part_table_name

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
        save_directory = os.getcwd() #current working directory
        archive_directory = os.path.join(save_directory, 'Archive') # Join the current working directory with the subdirectory 'Archive'
        
        pattern = os.path.join(save_directory, file_path)
        # Find all files matching the pattern
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

def read_excel_sheets(pattern):

    """
    Read sheets: 30c, 30d, 30e in an Excel file and return them as a list of tuples.

    Parameters:
        file_path (str): Path to the Excel file.

    Returns:
        list of tubles: A tuple where first elements are sheet names and second values are corresponding DataFrames.
    """
    global start_time
    try:
        start_time = time.time()
        # Find all files matching the pattern
        files = glob.glob(pattern)
        if not files:
            print("No files matching the pattern were found.")
            return []
        # Read all sheets into a dictionary of DataFrames
        for file in files:
            try:
                excel_data = pd.read_excel(file, sheet_name=['30c', '30d', '30e'], header=12) #like in V1(monshaat)
                logging.info("Started reading data")

            except FileNotFoundError:
                logging.error(f"File '{file_path}' not found.")
                return None
    
    except Exception as e:
        logging.error(f"An error occurred while reading Excel file: {str(e)}")
        return None
    
    try:
        # Convert dictionary to a list of tuples
        sheets_data = [(sheet_name, excel_data[sheet_name]) for sheet_name in excel_data]
        
        logging.info("Function of Reading all sheets reading finished")
        return sheets_data
    
    except Exception as e:
        logging.error(f"An error occurred while processing Excel sheets: {str(e)}")

def transform_data(sheets_data):
    """
    Transform raw data from Excel sheets into formatted DataFrames for different time periods.

    Args:
    - sheets_data (list): A list of tuples where each tuple contains a sheet name and its corresponding DataFrame.

    Returns:
    - transformed_data (dict): A dictionary where keys are table names and values are DataFrames with transformed data.

    Notes:
    - The function performs the following transformations:
      1. Extracts data starting from the row where 'الفترة' is found.
      2. Drops columns with all NaN values.
      3. Combines data from two columns into one.
      4. Delete unneeded column
      5. Renames columns based on the sheet name.
      6. Drops columns containing '_الفترة' in the name.
      7. Adds 'STG_CreatedDate' column with the current datetime.
      8. Splits 'Period' column into 'Yearnum' and 'Qurternum' columns for quarterly data.
      9. Filters rows based on specific patterns for yearly, quarterly, and monthly data.
      10. Stores each transformed DataFrame into a dictionary with the corresponding table name.

    """
    transformed_data = {}  # Dictionary to store transformed data

    for sheet_name, df in sheets_data:
        # Map sheet_name to part of table name
        part_table_name = mapping_sheet_name(sheet_name)
        try:
            logging.info(f"Transforming SAMA data: {sheet_name}...")
            # Find the index where 'الفترة' is found and start processing from there
            start_index = df.index[df['Unnamed: 1'] == 'الفترة'][0]
            df = df.iloc[start_index:]
            
            # Drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            
            # Combine data from two columns into one and delete second column
            df["Unnamed: 1"] = df["Unnamed: 1"].astype(str) + ' ' + df["Unnamed: 2"].astype(str)
            df['Unnamed: 1'] = df['Unnamed: 1'].str.replace("nan", "")
            del df["Unnamed: 2"]

            # Extract first row and all columns except the first one
            subset_data = df.iloc[0, 1:]  

            # Rename columns based on sheet_name
            if sheet_name == '30c':
                df.rename(columns=columns_30c, inplace=True)
                df.replace('---', np.nan, inplace=True)
            elif sheet_name == '30d':
                renamed_30d_columns = rename_30d(subset_data)
                df.columns = renamed_30d_columns
            elif sheet_name == '30e':
                renamed_30e_columns = rename_30e(subset_data)
                df.columns = renamed_30e_columns

            # Drop columns containing '_الفترة' in its name
            columns_to_drop = [col for col in df.columns if '_الفترة' in col]
            df.drop(columns=columns_to_drop, inplace=True)
        
            # Add 'STG_CreatedDate' column with the current datetime
            df['STG_CreatedDate'] = datetime.now()
            # replace nan values with '0'
            df.replace(np.nan, '0.0', inplace=True)

         
            # Identify the start and last index  where 'Period' column contains 'Q'
            first_Q_index = df[df['Period'].str.contains('Q1')].index.tolist()[0]
            last_Q_index = df[df['Period'].str.contains('Q1')].index.tolist()[-1]

            # Define patterns to match datetime formats
            year_pattern = r'^\d{4}|^\d{4}-12-31 00:00:00'
            month_pattern = r'^\d{4}-\d{2}-\d{2} 00:00:00'

            # year_df Filter rows based start dataframe to the first index has 'Q' & on  year_pattern format patterns
            year_df = df.loc[:first_Q_index - 1]
            year_df = year_df[year_df['Period'].astype(str).str.match(year_pattern)]

            """
            - If columns contain 'Number' in its name will be integer
            - If columns contain 'Sales' in its name will be float
            """
            year_number_columns = [col for col in year_df.columns if 'Number' in col]
            for col in year_number_columns:
                year_df[col] = year_df[col].apply(split_and_keep_integer).astype('Int64')

            year_sales_columns = [col for col in year_df.columns if 'Sales' in col]
            for col in year_sales_columns:
                year_df[col] = pd.to_numeric(year_df[col], errors='coerce').astype('Float64')
            
            """
                            quarter_df:
            - Filter the rows which have 'Q'
            - Split 'Period' column to 2 columns : Yearnum & Qurternum
            - Insert Yearnum as first column , insert Qurternum as second column
            - Remove from 'Yearnum' values '.0'
            - Drop 'Period'

            """

            quarter_df = df[df['Period'].str.contains('Q')]
            quarter_df.insert(quarter_df.columns.get_loc('Period') + 1, 'Qurternum',
                            df['Period'].str.split(pat=' ', expand=True)[0], allow_duplicates=False)
            quarter_df.insert(quarter_df.columns.get_loc('Period'), 'Yearnum',
                            df['Period'].str.split(pat=' ', expand=True)[1], allow_duplicates=False)
          
            # and the 'Year' column is of object type so '2000.0' -> '2000'
            quarter_df['Yearnum'] = quarter_df['Yearnum'].str.extract('(\d{4})', expand=False).astype(int)
            quarter_df.drop('Period', axis=1, inplace=True)

            quarter_number_columns = [col for col in quarter_df.columns if 'Number' in col]
            for col in quarter_number_columns:
                quarter_df[col] = quarter_df[col].apply(split_and_keep_integer).astype('Int64')

            quarter_sales_columns = [col for col in quarter_df.columns if 'Sales' in col]
            for col in year_sales_columns:
                quarter_df[col] = pd.to_numeric(quarter_df[col], errors='coerce').astype('Float64')

            # month_df Filter rows based start dataframe after the last index has 'Q' to the end of dataframe & on  month_pattern format patterns
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

            logging.info(f"Finished to transform SAMA data: {part_table_name}")
        
        except Exception as e:
            logging.error(f"An error occurred while transforming {sheet_name}: {str(e)}")
            continue #if sheet has a problem continue with another sheet

    return transformed_data

def load_transformed_dataframes(transformed_dataframes, dest_engine, schema_name):
    """
    Load the transformed dataframes into DB tables.

    Parameters:
        transformed_dataframes (dict): A dictionary where keys are sheet names and values are corresponding transformed DataFrames.
        dest_engine : engine created on destination table
        schema_name : scheam name where destination table located in

    Returns:
        total_execution_time (float): totla time in seconds from start reading data until loading to DB table
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
            #dest_count_df = e.read_database_count(db_name, schema_name, table_name, con=Engine)
            #count_of_dest = int(dest_count_df.iloc[0, 0])
            src_type = "EXCEL"
            #rejected_rows = 0
            #num_src = sum([df.shape[0] for df in data_frames])
            rejected_rows = 0          # num_src - count_of_dest
            e.Insert_TO_DMDQ(engine_dmdq, db_name, schema_name, table_name, execution_time, cols, rows, count, datetime.now(), src_table, src_type, rejected_rows)
            logging.info(f"Data load logged successfully for {table_name}.")
    except Exception as error:
        logging.error(f"Error logging data load: {error}")
        raise
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

def main():

    logging.info("Starting ETL process...")
    dest_config_key = 'ByFileDB_Extrenal_Prod'
    dmdq_config_key = 'ByDB_General_Prod' 
    file_path = "Monthly_Bulletin_*.xlsx"

    #if there is xlsx file in current working dir, start ETL process
    if check_for_xlsx_files(): 
        try:
            # Assuming establish_connections is correctly defined elsewhere
            Engine_DMDQ, Engine, SchemaName, database_name = establish_connections(dest_config_key, dmdq_config_key) 

            #read sheets in excel file and return list of tuples(sheet_name, dataframe)
        
            sheets_data = read_excel_sheets(file_path)
            # return dictionary, key=sheet_name & value= transformed dataframe
            transform_dfs = transform_data(sheets_data)
            # Load data to the database
            execution_time = load_transformed_dataframes(transform_dfs, Engine, SchemaName)
            # Log the data load operation
            log_data_load(Engine_DMDQ, database_name, SchemaName, list(transform_dfs.keys()), 'SAMA', execution_time, list(transform_dfs.values()))        
            logging.info(f"ETL process completed successfully in {execution_time} seconds.")
        
            #move file to 'Archive' after finished processing
            move_file_to_archive(file_path)
        except Exception as error:
            logging.error(f"An error occurred in the ETL process: {error}")
    else:
        logging.info("There is no new files to be processed")

if __name__ == '__main__':
    main()
