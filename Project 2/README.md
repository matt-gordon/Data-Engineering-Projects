# Project 2 - Data Modeling with Cassandra

## Purpose:

The purpose of this project is to create an ETL pipeline to enable the Sparkify analytics team to utilise song and user activty logs captured from our new music streaming app so as to better understand our users and support future app updates/business decisions. The current focus of the analytics team is to gain a better understanding of what songs users are listening to. This project will create an Apache Cassandra database with existing data stored in CSV files and create requested queries.

## Running:

Starting / Stoping a Cassandra server if running locally:
Start: "cassandra -f" (runs in foreground mode)
Stop: "ps -ax | grep cassandra" then find the PID for the running instance and then "kill <PID>" e.g. kill 908

## File Descriptions:

Project_1B_Project_Template.ipynb - Jupyter notebook containing the script to create Cassandra keyspace, process data and generate tables to support requested queries from the Sparkify Analytics Team.

## Queries

### 1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338 and itemInSession = 4

In order to generate data for this query, we need to create the following table using sessionId as a Partition Key and itemInSession as a Clustering Key.
  
| Column | Variable | Type |
|--------|---------------|---------|
| 1 | sessionId | int |
| 2 | itemInSession | int |
| 3 | artist_name | text |
| 4 | song_title | text |
| 5 | song_length | decimal |

### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

In order to generate data for this query, we need to create the following table using userId as a Partition Key and sessionId as a Clustering Key.

| Column | Variable      | Type |
| ------ | ------------- | ---- |
| 1      | userId        | int  |
| 2      | sessionId     | int  |
| 3      | itemInSession | int  |
| 4      | song_title    | text |
| 5      | artist_name   | text |
| 6      | user_name     | text |

### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

In order to generate data for this query, we need to create the following table using song_title as a Partition Key and user_name as a Clustering Key.  
  
| Column | Variable | Type |
|--------|------------|------|
| 1 | song_title | text |
| 2 | user_name | text |
