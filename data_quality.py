import configparser
import psycopg2
from sql_queries import data_quality_queries
import logging

def records_checks(cur, conn):
    """
    Check that all DWH tables have records.
    
    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for table_name in data_quality_queries.keys():

        # executing query and getting first record
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        records = cur.fetchone()
        num_records = records[0]

        # raising errors in case the query returns no records or returns 0
        if len(records) < 1:
            raise logging.error(f"Data quality check failed: table {table_name} returned no results.") 
        elif num_records < 1:
            raise logging.error(f"Data quality check failed: {table_name} contained 0 rows.")
        else:
            logging.info(f"Found {num_records} records on table {table_name}. Data quality check passed.")

def assert_null_checks(cur, conn):
    """
    Check that primary key columns have no null values.

    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for table_name, query in data_quality_queries.items():

        # executing query and getting first record
        cur.execute(query)
        records = cur.fetchone()
        num_records = records[0]

        # raising error in case the number of null values in the respective id column is not 0
        if num_records != 0:
            raise logging.error(f"Data quality check failed: id column on {table_name} has null values.")
        else:
            logging.info(f"No null values on id column on {table_name}.  Data quality check passed.")

def main():
    """
    Execution plan of etl.py script:
        1. Parse config and get Redshift DB settings
        2. Connect to Redshift DB
        3. Records check
        4. Primary keys check
        5. Close connection
    """

    logging.info("Parsing config with Redshift DB settings.") 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logging.info("Connecting to Redshift DB and getting cursor to it")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    logging.info("Checking that all DWH tables have records.")
    records_checks(cur, conn)

    logging.info("Checking that id columns on all DWH tables have no null values.")
    assert_null_checks(cur, conn)

    conn.close()
    logging.info("Connection closed.")


if __name__ == "__main__":

    # Will only be executed when this module is run directly.
    main()