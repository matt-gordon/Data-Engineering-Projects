"""
sql_queries.py

Purpose:  Define all Postgres commands used by create_tables.py and etl.py to create tables, drop tables,
        import data into the defined tables and run predefined queries.  This script does not contain 
        any executables.  Note import commands are not used by etl2.py.

Usage:    To use these commands in another script include " from sql_queries import * "
"""

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS  artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, user_id int NOT NULL, 
                        song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int NOT NULL, start_time timestamp NOT NULL, level varchar NOT NULL, 
                        location varchar NOT NULL, user_agent varchar NOT NULL);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar NOT NULL, 
                    last_name varchar NOT NULL, gender char NOT NULL, level varchar NOT NULL);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, artist_id varchar NOT NULL, title varchar NOT NULL, 
                    year int NOT NULL, duration numeric NOT NULL);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, artist_name varchar NOT NULL, artist_location varchar NOT NULL, 
                    artist_latitude numeric NOT NULL, artist_longitude numeric NOT NULL);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour int NOT NULL, day int NOT NULL, week int NOT NULL, 
                    month int NOT NULL, year int NOT NULL, weekday varchar NOT NULL);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (user_id, song_id, artist_id, session_id, start_time, level, location, user_agent)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s);""")

user_table_insert = ("""INSERT INTO  users (user_id, first_name, last_name, gender, level)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT  (user_id) DO UPDATE SET level = EXCLUDED.level;""")

song_table_insert = ("""INSERT INTO songs (song_id, artist_id, title, year, duration)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT  (song_id) DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (artist_id) DO NOTHING;""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id
               WHERE (songs.title = %s AND artists.artist_name = %s AND songs.duration = %s );""")



# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]