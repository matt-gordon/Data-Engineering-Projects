import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, clean_table_queries, modify_staging_events_table

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def clean_staging_tables(cur,conn):
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
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def show_tables(cur,conn):
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