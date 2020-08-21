import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Copies json and csv files from s3 bucket into staging tables on redshift
    using the queries in `copy_table_queries` list.
    
    Args:
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    Returns
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Queries data from staging tables and inserts into Eurotour fact and dimension tables
    using the queries in `insert_table_queries` list.
    
    Args:
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Parses config with redshift cluster settings
    
    - Establishes connection with the redshift cluster and gets cursor to it.  
    
    - Copies data from s3 to staging tables on redshift.  
    
    - Queries data from staging tables and inserts into fact and dimension tables. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()