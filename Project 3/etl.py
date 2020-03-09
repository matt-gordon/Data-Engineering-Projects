"""Main etl script to load data into Sparkify analytics fact and dimension tables

This etl script loads data from S3Bucket into staging tables on AWS Redshift, performs
basic data cleaning routines and then populates the Sparkify fact and dimension tables.
Successful execution of this program will result in the following tables being populated
with data on the Sparkify redshift cluster:
    fact_songplays, dim_songs, dim_users, dim_artists, dim_time
Refer to create_tables.py for table datatypes.

    Typical usage example:
        python etl.py

    Note: Redshift cluster must be running with access information defined in dwh.cfg and
    tables created using create_tables.py script.
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, clean_table_queries, modify_staging_events_table

def load_staging_tables(cur, conn):
    """Copies data stored in JSON files from S3 bucket into AWS Redshift staging tables

    Copies all JSON files found in S3 bucket location specified in dwh.cfg file to
    staging_events_table and staging_songs_table on running AWS Redshift cluster 
    specified in dwh.cfg.  Note the AWS Redshift cluster must already be running and
    tables created.  Copy statements are defined in sql_queries.py.

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  This function loads data into the AWS Redshift staging tables but 
        does not return any value.

    Raises:
        Undefined.  
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def clean_staging_tables(cur,conn):
    """Performs basic data cleaning on staging tables

    Executes a series of queries against the staging tables to remove any rows with blank 
    or 'None' as values for columns currently used to establish fact and dimension tables.
    Statements are defined in sql_queries.py. No data augmentation or smart in-fill is 
    attempted to reduce the amount of thrown out data.  After deletions complete VACUUM is 
    called on the staging tables to re-order and compress them.

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  This function peforms actions on the redshift cluster and returns no value.

    Raises:
        Undefined.  
    """
    for query in clean_table_queries:
        cur.execute(query)
        conn.commit()

    # In order to vacuum the tables after cleaning, the isolation level needs to be modified
    # Below routine attributed to 
    # https://stackoverflow.com/questions/1017463/postgresql-how-to-run-vacuum-from-code-outside-transaction-block
    old_isolation_level = conn.isolation_level
    new_isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    conn.set_isolation_level(new_isolation_level)

     #Vacuum staging tables after row deletions for missing data
    cur.execute("vacuum staging_songs_table;")
    conn.commit()
    cur.execute("vacuum staging_events_table;")
    conn.commit()

    # Restore old isolation level
    conn.set_isolation_level(old_isolation_level)
   
    # Add start_time column of dtype TIMESTAMP to staging_events_table
    cur.execute(modify_staging_events_table)
    conn.commit()

def insert_tables(cur, conn):
    """Inserts data from staging tables into fact and dimension tables

    Executes a series of queries against the staging tables to create the following
    tables: fact_songplays, dim_songs, dim_artists, dim_users, dim_time.
    Statements are defined in sql_queries.py

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  This function peforms actions on the redshift cluster and returns no value.

    Raises:
        Undefined.  
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def show_tables(cur,conn):
    """Prints the number of rows in each fact and dimension table

    Executes a query to count the number of entries in each table and then prints to console
    with the name of the table.

    Args:
        cur: established cursor to execute commands/queries on the redshift cluster
        conn: established redshift database connection
    
    Returns:
        Undefined.  Prints table name and number of entries to console for each table.

    Raises:
        Undefined.  
    """
    cur.execute("SELECT COUNT(*) FROM fact_songplays")
    num = cur.fetchone()[0]
    print("Total entries in fact_songplays: " + str(num))
    cur.execute("SELECT COUNT(*) FROM dim_songs")
    num = cur.fetchone()[0]
    print("Total entries in dim_songs: " + str(num))
    cur.execute("SELECT COUNT(*) FROM dim_users")
    num = cur.fetchone()[0]
    print("Total entries in dim_users: " + str(num))
    cur.execute("SELECT COUNT(*) FROM dim_artists")
    num = cur.fetchone()[0]
    print("Total entries in dim_artists: " + str(num))
    cur.execute("SELECT COUNT(*) FROM dim_time")
    num = cur.fetchone()[0]
    print("Total entries in dim_time: " + str(num))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected to cluster...")

    print("Loading Staging Tables...")
    load_staging_tables(cur, conn)

    print("Cleaning Staging Tables....")
    clean_staging_tables(cur,conn)

    print("Creating Fact & Dimension Tables...")
    insert_tables(cur, conn)

    print("Confirming Table Creation")
    show_tables(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()