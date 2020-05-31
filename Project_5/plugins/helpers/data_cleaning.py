class CleanData:
    clean_staging_events_table = ("""
                                DELETE FROM staging_events WHERE NULLIF(userid::VARCHAR,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(userid::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(firstname,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(firstname,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(lastname,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(lastname,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(gender,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(gender,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(length::VARCHAR,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(length::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(level,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(level,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(song,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(song,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(ts::VARCHAR,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(ts::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(location,'') IS NULL;
                                DELETE FROM staging_events WHERE NULLIF(location,'None') IS NULL;
""")
    
    clean_staging_songs_table = ("""
                                DELETE FROM staging_songs WHERE NULLIF(artist_location,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(artist_location,'None') IS NULL;
                                DELETE FROM staging_songs WHERE artist_location ~ '[0-9]{1}';
                                DELETE FROM staging_songs WHERE NULLIF(artist_latitude::VARCHAR,'None') IS NULL;  
                                DELETE FROM staging_songs WHERE NULLIF(artist_latitude::VARCHAR,'') IS NULL; 
                                DELETE FROM staging_songs WHERE NULLIF(artist_longitude::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_songs HERE NULLIF(artist_longitude::VARCHAR,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(artist_name,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(artist_name,'None') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(year,'0') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(title,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(title,'None') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(artist_id::VARCHAR,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(artist_id::VARCHAR,'None') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(song_id::VARCHAR,'') IS NULL;
                                DELETE FROM staging_songs WHERE NULLIF(song_id::VARCHAR,'None') IS NULL;
""")