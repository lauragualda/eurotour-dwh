import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging


def drop_tables(cur, conn):
    """
    Drops existing tables table using the queries in `drop_table_queries` list.
    
    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates non-existing table using the queries in `create_table_queries` list.
    
    Input
    -----
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Execution plan of create_tables.py script:
        1. Parse config and get Redshift DB settings
        2. Connect to Redshift DB
        3. Drop existing tables
        4. Created staging and DWH tables
        5. Close connection
    """

    logging.info("Parsing config with redshift cluster settings.") 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logging.info("Connecting to Redshift cluster and getting cursor to it")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    logging.info("Dropping existing tables according to list `drop_table_queries` on sql_queries.py")
    drop_tables(cur, conn)

    logging.info("Creating non-existing tables according to list `create_table_queries` on sql_queries.py")
    create_tables(cur, conn)

    conn.close()
    logging.info("Connection closed.")


if __name__ == "__main__":
    
    # Will only be executed when this module is run directly.
    main()