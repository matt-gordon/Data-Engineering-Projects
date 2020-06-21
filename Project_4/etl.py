"""Main etl script to load data into Sparkify analytics fact and dimension tables

This etl script loads data from S3Bucket into staging tables in Spark and then populates 
the Sparkify fact and dimension tables before saving back to an S3 bucket in parquet files.

    Typical usage example:
        spark-submit etl.py

    Note: Due to the amount of log event processing, it is recommended that this script is 
    run on an AWS EMR cluster.
"""

import os
import configparser
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *

# Get AWS access keys from the configuration file
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Establishes spark session
    Args:
        Nil
    Returns:
        spark session
    Raises:
        Undefined.  
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN') ## reduce the logging to remove INFO logs at runtime
    return spark


def process_song_data(spark, input_data, output_data):
    """Copies song data stored in JSON files from S3 bucket, processes and then outputs analytics to S3 

    Copies all JSON song data files found in S3 bucket specified into a song_table
    to create the songs and artists dimension tables.  These tables are then uploaded
    to output S3 bucket specified for the Analytics team.

    songs dimension table is partioned by year and artist_id.
    artist dimension table is not partitioned.
    
    Args:
        spark: established spark session
        input_data: S3 bucket location of song data
        output_data: S3 bucket location for saving analytics parquet files
    
    Returns:
        song_data dataframe to avoid having to reload data when creating songplays table.

    Raises:
        Undefined.  
    """
    # get filepath to song data file
    song_data =  input_data + 'song_data/*/*/*/*.json'
      
    # define the desired song schema
    song_schema = StructType([StructField('num_songs', IntegerType(), True),
                     StructField('artist_id', StringType(), True),
                     StructField('artist_latitude', DoubleType(), True),
                     StructField('artist_longitude', DoubleType(), True),
                     StructField('artist_location', StringType(), True),
                     StructField('artist_name', StringType(),True),
                     StructField('song_id', StringType(), True),
                     StructField('title', StringType(), True),
                     StructField('duration', DoubleType(), True),
                     StructField('year', IntegerType(), True)])
    
    print("reading song data from {}".format(song_data))

    # read song data file
    songs_df = spark.read.option("multiline","true").json(song_data, song_schema)
    songs_df = songs_df.dropna()
    print("{} entries loaded".format(songs_df.count()))
   
    # create a temp view of the song_data to run Spark SQL on
    songs_df.createOrReplaceTempView("song_table")

    print("creating songs table...")

    # extract columns to create songs table
    songs_table = spark.sql('''
                        SELECT DISTINCT song_id, 
                        artist_id, 
                        title, 
                        year, 
                        duration
                        FROM song_table
                        CLUSTER BY year, artist_id
                        ''')
    
    print("{} song entries".format(songs_table.count()))
    print("saving songs table to S3...")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data + 'songs/')

    print("creating artist table...")

    # extract columns to create artists table
    artists_table =  spark.sql('''
                            SELECT DISTINCT artist_id, 
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                            FROM song_table
                            ''')
    
    print(" {} artist entries".format(artists_table.count()))
    print("saving artist table to S3...")

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')

    return songs_df # return the song_data dataframe for use in process_log_data to generate songplays table.

def process_log_data(spark, input_data, songs_df, output_data):
    """Copies log event data stored in JSON files from S3 bucket, processes and then outputs analytics to S3 

    Copies all JSON log event data files found in S3 bucket specified into a log_table
    to create the songplays fact table, users and time dimension tables.  These tables are 
    then uploaded to output S3 bucket specified for the Analytics team.

    songplays fact table is partitioned by year and month.
    time dimension table is partioned by year and month.
    users dimension table is not partitioned.
    
    Args:
        spark: established spark session
        input_data: S3 bucket location of log data
        song_df: Dataframe of song data generated by process_song_data
        output_data: S3 bucket location for saving analytics parquet files
    
    Returns:
        Undefined.  This function generates tables and loads them back to S3 as parquet files

    Raises:
        Undefined.  
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    log_schema = StructType([StructField('artist', StringType(), False),
                     StructField('auth', StringType(), False),
                     StructField('firstName', StringType(), False),
                     StructField('gender', StringType(), False),
                     StructField('itemInSession', LongType(), False),
                     StructField('lastName', StringType(),False),
                     StructField('length', DoubleType(), False),
                     StructField('level', StringType(), False),
                     StructField('location', StringType(), False),
                     StructField('method', StringType(), False),
                     StructField('page', StringType(), False),
                     StructField('registration', DoubleType(), False),
                     StructField('sessionId', LongType(), False),
                     StructField('song', StringType(), False),
                     StructField('status', LongType(), False),
                     StructField('ts', LongType(), False),
                     StructField('userAgent', StringType(), False),
                     StructField('userId', StringType(), False)])

    print("reading log data from {}".format(log_data))

    # read log data file
    log_df = spark.read.option("multiline","true").json(log_data,log_schema)
    log_df = log_df.dropna()
    print("{} entries loaded".format(log_df.count()))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # create a temp view of the log_data to run Spark SQL on
    log_df.createOrReplaceTempView("log_table")
    songs_df.createOrReplaceTempView("song_table")

    print("creating time table...")

    # extract columns to create time table
    time_table = spark.sql('''
                    SELECT DISTINCT timestamp as start_time, 
                    hour(timestamp) as hour, 
                    day(timestamp) as day, 
                    weekofyear(timestamp) as week, 
                    month(timestamp) as month, 
                    year(timestamp) as year, 
                    date_format(timestamp, 'EEEE') as weekday
                    FROM log_table
                    CLUSTER BY year, month
                    ''')
    
    print("{} time entries".format(time_table.count()))
    print("saving time table to S3...") 

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + 'time')

    print("creating users table...")

     # extract columns for users table    
    users_table = spark.sql('''
                    SELECT log_table.userId as user_id, log_table.firstname as first_name, log_table.lastname as last_name, log_table.gender, log_table.level
                    FROM (
                        SELECT userId, max(ts) as ts
                        FROM log_table
                        GROUP BY userId) AS unique_users
                    INNER JOIN log_table
                    ON log_table.userId = unique_users.userId AND log_table.ts = unique_users.ts
                    ''')
    
    print("{} user entries".format(users_table.count()))
    print("saving users table to S3...")

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')

    print("creating songplays table...")
    song_df.createOrReplaceTempView("song_table") #Create a TempView for SQL query from input songs_data dataframe
  
    songplays_table = spark.sql('''
                            SELECT log.timestamp as start_time, log.userId, log.level, song.song_id, song.artist_id, log.sessionId, log.location, log.userAgent
                            FROM log_table as log
                            LEFT JOIN song_table as song 
                            ON log.song = song.title
                            AND log.artist = song.artist_name
                            AND log.length = song.duration
                            WHERE log.page = 'NextSong'
                            ''')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()) # add unique songplay_id

    print("{} songplay entries".format(songplays_table.count()))
    print("saving songplays table to S3...")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn("year",year('start_time')).withColumn("month",month('start_time')).write.mode("overwrite").partitionBy("year","month").parquet(output_data + 'songplays/')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4-gordon/"
    
    songs_data = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, songs_data, output_data)

if __name__ == "__main__":
    main()
