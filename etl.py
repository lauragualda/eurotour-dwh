import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import logging

def load_staging_tables(cur, conn):
    """
    Copy json and csv files from s3 bucket into staging tables on Redshift
    using the queries in `copy_table_queries` list.
    
    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Query data from staging tables and insert into Eurotour DWH tables
    using the queries in `insert_table_queries` list.
    
    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Execution plan of etl.py script:
        1. Parse config and get Redshift DB settings
        2. Connect to Redshift DB
        3. Load data to staging tables
        4. Load data to DWH tables
        5. Close connection
    """

    logging.info("Parsing config with Redshift DB settings.") 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logging.info("Connecting to Redshift DB and getting cursor to it")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    logging.info("Dropping existing tables according to list `copy_table_queries`on sql_queries.py")
    load_staging_tables(cur, conn)

    logging.info("Creating non-existing tables according to list `insert_table_queries` on sql_queries.py")
    insert_tables(cur, conn)

    conn.close()
    logging.info("Connection closed.")


if __name__ == "__main__":

    # Will only be executed when this module is run directly.
    main()