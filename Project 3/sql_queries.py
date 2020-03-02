import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table
                                (
                                    artist VARCHAR NOT NULL,
                                    auth VARCHAR NOT NULL
                                    firstName VARCHAR NOT NULL,
                                    gender CHAR NOT NULL,
                                    itemInSession INT NOT NULL,
                                    lastName VARCHAR NOT NULL,
                                    length NUMERIC NOT NULL,
                                    location VARCHAR NOT NULL,
                                    method VARCHAR(3) NOT NULL,
                                    page VARCHAR NOT NULL,
                                    registration FLOAT NOT NULL,
                                    sessionId INT NOT NULL,
                                    song VARCHAR NOT NULL,
                                    status INT NOT NULL,
                                    ts INT NOT NULL,
                                    userAgent VARCHAR NOT NULL,
                                    userId INT NOT NULL
                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                (
                                    num_songs INT NOT NULL,
                                    artist_id VARCHAR NOT NULL,
                                    artist_latitude NUMERIC NOT NULL,
                                    artist_longitude NUMERIC NOT NULL,
                                    artist_location VARCHAR NOT NULL,
                                    artist_name VARCHAR NOT NULL,
                                    song_id VARCHAR NOT NULL,
                                    title VARCHAR NOT NULL,
                                    duration NUMERIC NOT NULL,
                                    year INT NOT NULL
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                        (
                            songplay_id serial PRIMARY KEY, 
                            user_id int NOT NULL, 
                            song_id varchar NOT NULL, 
                            artist_id varchar NOT NULL, 
                            session_id int NOT NULL,
                            start_time timestamp NOT NULL, 
                            level varchar NOT NULL, 
                            location varchar NOT NULL, 
                            user_agent varchar NOT NULL)
                        ;
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                    (
                        user_id int PRIMARY KEY, 
                        first_name varchar NOT NULL, 
                        last_name varchar NOT NULL, 
                        gender char NOT NULL, 
                        level varchar NOT NULL
                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                    (
                        song_id varchar PRIMARY KEY, 
                        artist_id varchar NOT NULL, 
                        title varchar NOT NULL, 
                        year int NOT NULL, 
                        duration numeric NOT NULL
                    );
""")

artist_table_create =   ("""CREATE TABLE IF NOT EXISTS artists 
                        (
                            artist_id varchar PRIMARY KEY, 
                            artist_name varchar NOT NULL, 
                            artist_location varchar NOT NULL, 
                            artist_latitude numeric NOT NULL, 
                            artist_longitude numeric NOT NULL
                        );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                    (
                        start_time timestamp PRIMARY KEY, 
                        hour int NOT NULL, 
                        day int NOT NULL, 
                        week int NOT NULL, 
                        month int NOT NULL, 
                        year int NOT NULL, 
                        weekday varchar NOT NULL
                        );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
