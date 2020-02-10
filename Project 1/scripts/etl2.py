from io import StringIO
import os
import sys
import glob
import psycopg2
from sql_queries import *
import pandas as pd

def get_files(filepath):
    r"""
    Create a list of filepaths to all .json files at the provided directory

    Keyword Arguments:
    filepath - relative filepath to the directory to search for all .json files.
    e.g. "data/songdata"

    Outputs:
    Prints to console the total number of files found and the directory location searched,
    e.g. "10 files found in data/songdata"

    Returns:
    all_files - list containing absolute file path to all .json files found in directory

    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    return all_files

def process_song_files(file_list):
    """
    Loads all json files provided in file_list into a DataFrame, conducts basic data cleaning (removing rows with ANY missing data) and creates DataFrames for the 
    songs and artists tables.

    Keyword Arguments:
    file_list - list of absolute paths to all song data json files to be imported into the songs and artists tables.

    Outputs:
    Prints to console the progress of json file import.

    Returns (in order):
    song_data - Pandas DataFrame formatted to align with the import order of the songs table (song_id, artist_id, title, year and duration)
    artists_data - Pandas DataFrame formatted to align with the import order of the artists table (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)

    """

    # iterate over file list and import into Pandas DataFrame
    num_files = len(file_list)

    for i, datafile in enumerate(file_list, 0):
        if i == 0: # if this is the first iteration, create the dataframe otherwise append the latest file results
            df = pd.DataFrame(pd.read_json(datafile,lines=True))
            print('{}/{} files processed.'.format(i+1, num_files))
        else:
            df = df.append(pd.read_json(datafile,lines=True))
            print('{}/{} files processed.'.format(i+1, num_files))

    # Apply basic data cleaning noted during inspection of results.  Currently dropping any row that has 'None' or 'NaN' in any column and any row where year = 0 (incorrect value)
    df = df.reset_index(drop=True) # reset the index of the dataframe (import process results in all indices showing 0)
    df = df.dropna()
    # Get names of indices for which column year has value 0
    drop_indices = df[ df['year'] == 0 ].index
    # Delete these row indices from dataFrame
    df.drop(drop_indices, inplace=True)

    # create songs dataframe
    song_data = df[['song_id','artist_id','title','year','duration']]   

    # create artists dataframe
    artist_data  = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
  
    return song_data, artist_data


def process_log_files(cur, file_list):
    """
    Loads all json files provided in file_list into a DataFrame, conducts basic data cleaning (removing rows with ANY missing data) and creates DataFrames for the
    time, users and songplays tables.

    Keyword Arguments:
    cur - established Postgres cursor to execute commands/queries
    file_list - list of absolute paths to all log data json files to be imported into the time, users and songplay tables.

    Outputs:
    Prints to console the progress of json file import.

    Returns (in order):
    time_data - Pandas DataFrame formatted to align with the import order of the time table (start_time, hour, day, week, year, weekday)
    user_data - Pandas DataFrame formatted to align with the import order of the users table (user_id, first_name, last_name, gender, level)
    songplays_data - Pandas DataFrame formatted to align with the import order of the songplays table (user_id, song_id, artist_id, session_id, start_time, level, location, user_agent)
    

    """
    num_files = len(file_list)

    #iterate over files and process
    for i, datafile in enumerate(file_list, 0):
        if i == 0: #if this is the first iteration, create the dataframe otherwise append the latest file results
            df = pd.DataFrame(pd.read_json(datafile,lines=True))
            print('{}/{} files processed.'.format(i+1, num_files))
        else:
            df = df.append(pd.read_json(datafile,lines=True))
            print('{}/{} files processed.'.format(i+1, num_files))

    # filter by NextSong action
    df = df[df['page']=="NextSong"]

    # convert timestamp column to datetime
    df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
  
    # insert time data records
    time_data = ([[row,row.hour,row.day,row.week,row.month,row.year, row.day_name()] for row in df['datetime']])
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_data = pd.DataFrame(time_data, columns = column_labels).drop_duplicates(subset='start_time')    # bulk import process can't use ON CONFLICT so ensure duplicates removed beforehand

    # load user table
    user_data = df[['userId','firstName','lastName','gender','level']].copy()
    user_data.userId = user_data.userId.astype(int)
    user_data = user_data.drop_duplicates(subset='userId') # bulk import process can't use ON CONFLICT so ensure duplicates removed beforehand
    user_data.columns = ["user_id","first_name","last_name", "gender","level"]
    
    # insert songplay records
    df = df.reset_index(drop=True)
    print("creating songplays")
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # create songplay dataframe
        if index == 0: 
            songplay_data = pd.DataFrame([[row['userId'],songid,artistid,row['sessionId'],row['datetime'],row['level'],row['location'],row['userAgent']]])
        else:
            songplay_data = songplay_data.append([[row['userId'],songid, artistid, row['sessionId'],row['datetime'],row['level'],row['location'],row['userAgent']]])

     #align column names with query names & reset the index    
    songplay_data.columns = ["user_id","song_id","artist_id","session_id","start_time","level","location","user_agent"]
    songplay_data = songplay_data.reset_index(drop=True)
    print("songplays created")

    return time_data, user_data, songplay_data

def bulk_import(cur, conn, table_name, dataframe):
    """
    Bulk import provided DataFrame into Postgres table

    Keyword Arguments:
    cur - established Postgres cursor to execute commands/queries
    conn - established Postgres database connection
    table_name - name of the Postgres table the dataframe is being imported to.  Case-sensitive.
    dataframe - pandas dataframe containing the formatted data to be imported.  The column order must be in the same order as the Postgres table is defined.

    Outputs:
    None

    Returns:
    None
    
    """
    # This routine is based upon ellisvalentiner/bulk-insert.py routine at https://gist.github.com/ellisvalentiner/63b083180afe54f17f16843dd51f4394

    # Intialise a string buffer
    sio = StringIO()
    # Write the DataFrame as a csv to the buffer
    sio.write(dataframe.to_csv(index=None, header=None, sep = '\t')) # As the data contains or is likely to contain ',', use tab delimeter for csv
    #Set the position to the start of the stream
    sio.seek(0)
    # Copy the string buffer to the database, as if it were an actual file
    cur.copy_from(sio, table_name, columns=dataframe.columns, sep='\t')
    conn.commit()
  

if __name__ == "__main__":
    # Establish connection to the sparkifydb
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except Exception as e:
        print("Error establishing connection to database, please check settings and try again")
        print(e)
        conn.close()
        sys.exit(1)  # stop processing remainder of steps and exit rather than throw each subsequent exception

    # Create list of song files and log files in the data directory
    try:
        song_files = get_files(filepath='../data/song_data')
        log_files = get_files(filepath='../data/log_data')
    except Exception as e:
        print("Error creating one or more of the file lists.  Please check your settings and try again")
        print(e)
        conn.close()
        sys.exit(1)  # stop processing remainder of steps and exit rather than throw each subsequent exception

    # Create the songs, artists, time, users and songplays dataframes to be imported into Postgres DB
    try:
        song_data, artist_data = process_song_files(file_list=song_files)
        time_data, user_data, songplay_data = process_log_files(cur, file_list=log_files)
    except Exception as e:
        print("Error creating data tables")
        print(e)
        conn.close()
        sys.exit(1)  # stop processing remainder of steps and exit rather than throw each subsequent exception

    # Bulk import each dataframe into their respective table
    try:
        bulk_import(cur, conn, 'songs', song_data)
        print("songs table imported")
        bulk_import(cur, conn, 'artists', artist_data)
        print("artists table imported")
        bulk_import(cur, conn, 'time', time_data)
        print("time table imported")
        bulk_import(cur, conn, 'users', user_data)
        print("users table imported")
        bulk_import(cur, conn, 'songplays', songplay_data)
        print("songplays table imported")
    except Exception as e:
        print("Error importing data into tables.  Please check your datatables and try again")
        print(e)
        conn.close()
        sys.exit(1)  # stop processing remainder of steps and exit rather than throw each subsequent exception
  
    conn.close()
