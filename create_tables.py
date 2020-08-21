import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops existing tables table using the queries in `drop_table_queries` list.
    
    Args:
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates non-existing table using the queries in `create_table_queries` list.
    
    Args:
        conn: psycopg2 connection to a redshift DB
        cur: psycopg2 cursor
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Parses config with redshift cluster settings
    
    - Establishes connection with the redshift cluster and gets cursor to it.  
    
    - Drops existing the tables.  
    
    - Creates non-existing tables needed. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()