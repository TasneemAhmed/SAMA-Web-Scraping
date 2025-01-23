"""
This script provides utility functions for database operations across SQL Server, MySQL, and PostgreSQL. 
It includes functionalities to connect to databases, read and manipulate data, and maintain load frequency counts.
"""

import urllib.parse
import sqlalchemy
from sqlalchemy import create_engine, text, literal_column
import pandas as pd
import logging
import mysql.connector 
import psycopg2

import ETL_Config as c


def Connect_TO_SQL(TargetServer: str, TargetDb: str, username: str, password: str) -> sqlalchemy.engine.Engine:
    """
    Connects to a SQL Server database using provided credentials.
    
    Args:
        TargetServer (str): Server address.
        TargetDb (str): Database name.
        username (str): Username for the database.
        password (str): Password for the database.

    Returns:
        sqlalchemy.engine.Engine: A connection engine to the SQL Server database.
    """
    try:
        params = urllib.parse.quote_plus(
            f"DRIVER={{SQL Server}};SERVER={TargetServer};DATABASE={TargetDb};UID={username};PWD={password}"
        )
        conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
        return create_engine(conn_str, encoding="utf-8")
    except Exception as e:
        logging.exception("Error connecting to SQL Server: %s", e)
        raise


# connect to destinations 
def connect_to_databases(dest_config_key: str, dmdq_config_key: str):
    """
    Establishes connections to DM_Quality and a variable Destination database using configurations from ETL_Config.
    
    Args:
        dest_config_key (str): Key to specify which Destination database configuration to use.

    Returns:
        tuple: Tuple containing engine objects for DM_Quality and the specified Destination database.
    """
    try:
        config_DMDQ = c.config["servers"][dmdq_config_key]
        Engine_DMDQ = Connect_TO_SQL(config_DMDQ["server"], config_DMDQ["database"], 
                                     config_DMDQ["username"], config_DMDQ["password"])

        config_dest = c.config["servers"][dest_config_key]
        Engine_Dest = Connect_TO_SQL(config_dest["server"], config_dest["database"], 
                                     config_dest["username"], config_dest["password"])

        return Engine_DMDQ, Engine_Dest
    except Exception as e:
        logging.exception("Error connecting to databases: %s", e)
        raise


def create_mysql_connection(config_key: str, port: int = None, auth_plugin: str = None):
    """
    Creates and returns a MySQL connection using the specified configuration.
    
    Args:
        config_key (str): The key to access the database configuration.
        port (int, optional): The port number for the database connection.
        auth_plugin (str, optional): The authentication plugin for the database connection.

    Returns:
        MySQLConnection: A MySQL connection object.
    """
    config = c.config["servers"][config_key]
    connection_params = {
        "host": config["server"],
        "database": config["database"],
        "user": config["username"],
        "passwd": config["password"],
        "use_pure": True
    }

    if port:
        connection_params["port"] = port
    if auth_plugin:
        connection_params["auth_plugin"] = auth_plugin

    return mysql.connector.connect(**connection_params)


def create_postgres_connection(config_key: str, port: int = None, sslmode: str = None):
    """
    Creates and returns a PostgreSQL connection using the specified configuration.
    
    Args:
        config_key (str): The key to access the database configuration.
        port (int, optional): The port number for the database connection.
        sslmode (str, optional): The SSL mode for the database connection.

    Returns:
        psycopg2.extensions.connection: A PostgreSQL connection object.
    """
    config = c.config["servers"][config_key]
    connection_params = {
        "host": config["server"],
        "dbname": config["database"],
        "user": config["username"],
        "password": config["password"]
    }

    if port:
        connection_params["port"] = port
    if sslmode:
        connection_params["sslmode"] = sslmode

    return psycopg2.connect(**connection_params)

def create_mssql_connection(config_key: str):
    """
    Creates and returns a MSSQL connection using the specified configuration.
    
    Args:
        config_key (str): The key to access the database configuration.
    Returns:
        mssql.extensions.connection: A MSSQL connection object.
    """
    try:

        config_src = c.config["servers"][config_key]
        Engine_src = Connect_TO_SQL(config_src["server"], config_src["database"], 
                                     config_src["username"], config_src["password"])

        return  Engine_src
    except Exception as e:
        logging.exception("Error connecting to databases: %s", e)
        raise

def read_source_data(table_name: str, connection) -> pd.DataFrame:
    """
    Reads data from a specified source table and returns it as a DataFrame.
    
    Args:
        table_name (str): Name of the source table.
        connection (sqlalchemy.engine.Connection): Database connection object.

    Returns:
        pd.DataFrame: DataFrame containing data from the source table.
    """
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, connection)


def read_database_count(db_name: str, schema_name: str, table_name: str, con):
    """
    Executes a SELECT count(*) query for a given table and returns results.
    
    Args:
        db_name (str): Name of the database.
        schema_name (str): Schema name in the database.
        table_name (str): Table name.
        con: Connection object to the database.

    Returns:
        The count of rows in the specified table.
    """
    try:
        query = f"SELECT count(*) FROM {db_name}.{schema_name}.{table_name}"
        return pd.read_sql(query, con)
    except Exception as e:
        logging.exception("Error executing read query: %s", e)
        raise


def truncate_table(engine: sqlalchemy.engine.Engine, Db: str, schema: str, table: str):
    """
    Truncates the specified table in the database.
    
    Args:
        engine: SQLAlchemy engine connected to the database.
        Db (str): Database name.
        schema (str): Schema name.
        table (str): Table name.
    """
    try:
        connection = engine.connect().execution_options(isolation_level="AUTOCOMMIT") 
        connection.execute(f"TRUNCATE TABLE {Db}.{schema}.{table}")
    except Exception as e:
        logging.exception("Error truncating table: %s", e)
        raise


def Generate_Frequency_of_load(engine, source_table) -> int:
    """
    Generates and updates load frequency count for a specified table.
    
    Args:
        engine: SQLAlchemy engine connected to the database.
        source_table (str): Name of the source table.

    Returns:
        int: The next load count as an integer.
    """
    query = text("""
        SELECT Max_Load_Count as next_count
        FROM ByDB.[General].Frequency_of_load_count
        WHERE DB_Table = :source_table
    """)

    with engine.connect() as connection:
        result = connection.execute(query, source_table=source_table)
        row = result.mappings().first()
        count = 0

        if row is None:
            count = 1
            insert_query = text("""
                INSERT INTO ByDB.[General].Frequency_of_load_count (DB_Table, Max_Load_Count, Insertion_date) 
                VALUES (:source_table, :count, GETDATE())
            """)
            connection.execute(insert_query, source_table=source_table, count=count)
        else:
            count = int(row['next_count']) + 1
            update_query = text("""
                UPDATE ByDB.[General].Frequency_of_load_count 
                SET Max_Load_Count = :count
                WHERE DB_Table = :source_table
            """)
            connection.execute(update_query, source_table=source_table, count=count)

        return count


def Insert_TO_DMDQ(Engine_DMDQ, db_name: str, db_schema: str, db_table: str,
                   Time_of_exe: str, cols: int, rows: int, count: int, date, src_table: str,
                   src_type: str, no_of_rejected_rows: int):
    """
    Inserts a record into the DM_Quality table.
    
    Args:
        Engine_DMDQ: SQLAlchemy engine connected to DM_Quality.
        db_name (str): Database name.
        db_schema (str): Schema name.
        db_table (str): Table name.
        Time_of_exe (str): Execution time.
        cols (int): Number of columns in the data.
        rows (int): Number of rows in the data.
        count (int): Frequency count.
        date: Date of the operation.
        src_table (str): Source table name.
        src_type (str): Source type (e.g., file, database).
        no_of_rejected_rows (int): Number of rows rejected during processing.
    """
    try:
        query = """
              INSERT INTO ByDB.[General].DM_Quality (DB_Name,DB_Schema,DB_Table,Time_of_execution,Number_of_Columns,Number_of_Rows,Frequency_of_load,STG_CreatedDate,SRC_Table,SRC_Type,Number_of_Rejected_Rows)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?)
        """
        Engine_DMDQ.execute(query, 
                            db_name, db_schema, db_table, 
                            Time_of_exe, cols, rows, count, 
                            date,src_table, src_type, no_of_rejected_rows)
    except Exception as e:
        logging.exception("Error inserting to DM_Quality: %s", e)
        raise

# Logging configuration
logging.basicConfig(level=logging.INFO)
