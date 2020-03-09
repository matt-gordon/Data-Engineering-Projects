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
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender CHAR,
                                    itemInSession INT,
                                    lastName VARCHAR,
                                    length VARCHAR,
                                    level VARCHAR(4),
                                    location VARCHAR,
                                    method VARCHAR(3),
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId BIGINT,
                                    song VARCHAR,
                                    status INT,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId INT
                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
                                (
                                    num_songs INT,
                                    artist_id VARCHAR,
                                    artist_latitude VARCHAR,
                                    artist_longitude VARCHAR,
                                    artist_location VARCHAR,
                                    artist_name VARCHAR,
                                    song_id VARCHAR,
                                    title VARCHAR,
                                    duration VARCHAR,
                                    year INT
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS fact_songplays 
                        (
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                            user_id BIGINT NOT NULL, 
                            song_id VARCHAR NOT NULL, 
                            artist_id VARCHAR NOT NULL, 
                            session_id BIGINT NOT NULL,
                            start_time TIMESTAMP NOT NULL, 
                            level VARCHAR NOT NULL, 
                            location VARCHAR NOT NULL, 
                            user_agent VARCHAR NOT NULL
                        );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users 
                    (
                        user_id BIGINT PRIMARY KEY, 
                        first_name VARCHAR NOT NULL, 
                        last_name VARCHAR NOT NULL, 
                        gender CHAR NOT NULL, 
                        level VARCHAR(4) NOT NULL
                    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs 
                    (
                        song_id VARCHAR PRIMARY KEY, 
                        artist_id VARCHAR NOT NULL, 
                        title VARCHAR NOT NULL, 
                        year INT NOT NULL, 
                        duration FLOAT8 NOT NULL
                    );
""")

artist_table_create =   ("""CREATE TABLE IF NOT EXISTS dim_artists 
                        (
                            artist_id VARCHAR PRIMARY KEY, 
                            artist_name VARCHAR NOT NULL, 
                            artist_location VARCHAR NOT NULL, 
                            artist_latitude FLOAT8 NOT NULL, 
                            artist_longitude FLOAT8 NOT NULL
                        );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time 
                    (
                        start_time TIMESTAMP PRIMARY KEY, 
                        hour INT NOT NULL, 
                        day INT NOT NULL, 
                        week INT NOT NULL, 
                        month INT NOT NULL, 
                        year INT NOT NULL, 
                        weekday VARCHAR NOT NULL
                    );
""")

# STAGING TABLES

staging_events_copy =  ("""
                            COPY staging_events_table FROM {}
                            iam_role {}
                            json {};
                        """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy =    ("""
                            COPY staging_songs_table FROM {}
                            iam_role {}
                            json 'auto';
                        """).format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO fact_songplays (user_id, song_id, artist_id, session_id, start_time, level, location, user_agent)
                            SELECT set.userid as user_id, sst.song_id as song_id, sst.artist_id as artist_id, set.sessionid as session_id, 
                                    set.start_time as start_time, set.level as level, set.location as location, set.useragent as user_agent
                            FROM staging_events_table AS set
                            JOIN staging_songs_table AS sst
                            ON (set.artist = sst.artist_name)
                            AND (set.song = sst.title)
                            AND (cast(set.length as float8) = cast(sst.duration as float8))
                            WHERE set.page = 'NextSong'
""")

# Redshift doesn't have ON CONSTRAINT type functionality and needed the ability to find if there were duplicate entries of user_id, 
# to return only the most recent one when building the table so that when querying the user table, we'd see the latest status for that user.  
# The below query was based upon approach learnt in a blog by Jack Wilson https://thoughtbot.com/blog/ordering-within-a-sql-group-by-clause

user_table_insert = ("""
                        INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
                        SELECT staging_events_table.userid as user_id, staging_events_table.firstname as first_name, 
                                staging_events_table.lastname as last_name, staging_events_table.gender, staging_events_table.level
                        FROM (
                                SELECT userid, MAX(ts) AS ts
                                FROM staging_events_table
                                GROUP BY userid) AS unique_users
                        INNER JOIN  staging_events_table
                        ON staging_events_table.userid = unique_users.userid AND staging_events_table.ts = unique_users.ts;
""")

song_table_insert = ("""
                        INSERT INTO dim_songs (song_id, artist_id, title, year, duration)
                        SELECT DISTINCT sst.song_id, sst.artist_id, sst.title, sst.year, cast(sst.duration as float8)
                        FROM staging_songs_table as sst;
""")

artist_table_insert = ("""
                        INSERT INTO dim_artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                        SELECT DISTINCT sst.artist_id, sst.artist_name, sst.artist_location, cast(sst.artist_latitude as float8), 
                                        cast(sst.artist_longitude as float8)
                        FROM staging_songs_table as sst;
""")

time_table_insert = ("""
                        INSERT INTO dim_time (start_time,hour,day,week,month,year,weekday)
                        SELECT DISTINCT set.start_time,
                        EXTRACT(HOUR FROM set.start_time) as hour,
                        EXTRACT(DAY FROM set.start_time) as day,
                        EXTRACT(WEEK FROM set.start_time) as week,
                        EXTRACT(MONTH FROM set.start_time) as month,
                        EXTRACT(YEAR FROM set.start_time) as year,
                        EXTRACT(WEEKDAY FROM set.start_time) as weekday
                        FROM staging_events_table as set;
""")

# BASIC DATA CLEANING QUERY 
# Remove all rows with missing data or 'None' in fields used in current fact and dimension tables.  Future updates could look to further error check
# and augment the data to reduce the amount of log and song records thrown out.

clean_staging_events_table = ("""
                                DELETE FROM staging_events_table WHERE NULLIF(userid::VARCHAR,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(userid::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(firstname,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(firstname,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(lastname,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(lastname,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(gender,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(gender,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(length,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(length,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(level,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(level,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(song,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(song,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(ts::VARCHAR,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(ts::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(location,'') IS NULL;
                                DELETE FROM staging_events_table WHERE NULLIF(location,'None') IS NULL;
""")

clean_staging_songs_table = ("""
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_location,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_location,'None') IS NULL;
                                DELETE FROM staging_songs_table WHERE artist_location ~ '[0-9]{1}';
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_latitude,'None') IS NULL;  
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_latitude,'') IS NULL; 
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_longitude,'None') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_longitude,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_name,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_name,'None') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(year,'0') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(title,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(title,'None') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_id,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(artist_id,'None') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(song_id,'') IS NULL;
                                DELETE FROM staging_songs_table WHERE NULLIF(song_id,'None') IS NULL;
""")

# MODIFY staging_events_table to include proper timestamp format by creating new column 'start_time
modify_staging_events_table = ("""
                                ALTER TABLE staging_events_table ADD COLUMN start_time TIMESTAMP default NULL;
                                UPDATE staging_events_table SET start_time = (SELECT TIMESTAMP 'epoch' + ts/1000*INTERVAL '1 second');
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
clean_table_queries = [clean_staging_songs_table, clean_staging_events_table]
