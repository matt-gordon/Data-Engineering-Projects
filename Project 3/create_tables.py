"""Drops tables if they exist and then creates new tables as defined in sql_queries.py

Program establishes a connection to the running Redshift cluster using parameters defined
in dwh.cfg.  To ensure clean, empty tables are used all required Sparkify analytics tables
are dropped if they exist and then new tables are created.

    Typical usage:
        python create_tables.py
    
    Note: This must be run before etl.py to ensure tables exist and are empty.
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all Sparkify staging, fact and dimension tables from Redshift cluster.

    Executes all drop table statements defined in sql_queries.py

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  

    Raises:
        Undefined.  
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all Sparkify staging, fact and dimension tables on Redshift cluster.

    Executes all create table statements defined in sql_queries.py

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  

    Raises:
        Undefined.  
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to cluster...")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Tables created...")

    conn.close()

if __name__ == "__main__":
    main()