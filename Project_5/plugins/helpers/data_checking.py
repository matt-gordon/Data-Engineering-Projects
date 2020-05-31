class DataQuality: 
        ## Initial demo data quality checks to demonstrate functionality.
        ## Note these all check the primary keys of each table which by default cannot be null
        check_songplays = ('''
                            SELECT COUNT(*) 
                            FROM factSongplays
                            WHERE playid is NULL
                            ''')
        
        check_songs = ('''
                        SELECT COUNT(*)
                        FROM dimSongs
                        WHERE songid is NULL
                        ''')
        
        check_artists = ('''
                        SELECT COUNT(*)
                        FROM dimArtists
                        WHERE artistid is NULL
                        ''')
        
        check_users = ('''
                        SELECT COUNT(*)
                        FROM dimUsers
                        WHERE userid is NULL
                        ''')
        
        check_time = ('''
                        SELECT COUNT(*)
                        FROM dimTime
                        WHERE start_time is NULL
                        ''')
        
        test_queries = [check_songplays, check_songs, check_artists, check_users, check_time]
        test_results = [0,0,0,0,0]
        