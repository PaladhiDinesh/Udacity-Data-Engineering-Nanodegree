#S3 DataLake

##Motive:

Aim of this project is to create an ETL pipeline using the data stored in S3 buckets and process that data into respective Fact and dimentsion tables using spark and then upload the parquet files back to S3.

##Prerequisite

1. Install [Python 3.x](https://www.python.org/).
2. Also install **conda** and **pyspark**

##Dataset (Public S3 buckets):
Song Data: s3://udacity-dend/song_data 
Log Data Path: s3://udacity-dend/log_data 

<b>Schema </b>

A Star Schema would be required for optimized queries on song play queries

<b>Fact Table</b>

<b>songplays_table</b> - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users_schema_table</b> - users in the app user_id, first_name, last_name, gender, level

<b>songs_schema_table</b> - songs in music database song_id, title, artist_id, year, duration

<b>artists_schema_table</b> - artists in music database artist_id, name, location, lattitude, longitude

<b>time_schema_table</b> - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

##Pipeline

1.Load the Data which are in JSON Files(Song Data and Log Data)
2.Use Spark to process these JSON files and then generate a set of Fact and Dimension Tables
4.Load back this data to S3


##Execution Steps:
1.Insert your AWS IAM credential in dl.cfg
2.Run etl.py in terminal
