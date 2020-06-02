# Project 5 - Data Pipelines with Airflow

## Purpose:

The purpose of this project is to establish a Data Pipeline using Airflow to automate the ETL process on AWS to create reliable, fresh data analytics tables to support Sparkify analytics team better understand our users and support future app updates/business decisions.

## Running:

### DAG and Operator files

- Ensure dags, helper and custom operators (along with their _init_.py files) are in their respective folders of the Airflow instance being used.

### Create Connections

- Prior to running the 'sparkify_dag' DAG, ensure the following connections have been setup:

![Connections](./Resources/admin-connections.png)

1.  aws_credentials:  
    Conn Id: Enter aws_credentials.  
    Conn Type: Enter Amazon Web Services.  
    Login: Enter your Access key ID from the IAM User credentials.  
    Password: Enter your Secret access key from the IAM User credentials.

![AWS Credentials](./Resources/connection-aws-credentials.png)

2.  redshift:  
    Conn Id: Enter redshift.  
    Conn Type: Enter Postgres.  
    Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.  
    Schema: Enter dev. This is the Redshift database you want to connect to.  
    Login: Enter awsuser.  
    Password: Enter the password you created when launching your Redshift cluster.  
    Port: Enter 5439.

Note: The redshift cluster needs to be created in the same region as the S3 bucket being used.

![Redshift Cluster Connection Details](./Resources/connection-redshift.png)

## DAG Descriptions:

![Sparkify_DAG](./Resources/sparkify_dag_image.png)

Start_execution:  
Stage_events:  
Stage_songs:  
Load_songplays_fact_table:  
Load_user_dim_table:  
Load_song_dim_table:  
Load_artist_dim_table:  
Load_time_dim_table:  
Run_data_quality_checks:  
Stop_execution:  


## Staging Tables

As an intermediate step, the JSON log and song files are initially loaded into staging_events and staging_songs respectively. Basic data integrity/cleaning checks are made prior to loading the data into the Fact and Dimension tables.

![Staging Tables](./Resources/Project5_staging_tables.png)

## Relational Database Structure

Based upon the available data and needs of Sparkify, the following Postgres database design was utilised containing one Fact Table (songplays) and four Dimension Tables (users, artists, songs and time). The Star Schema representation is shown below. <br>

![Star Schema](./Resources/Project5_star_schema.png)

The tables were generated as per the Project specification. It is noted that they are almost normalised, with the exception of 'level' not being a primary key in the users table, yet being duplicated in the songplays table. This duplication should be investigated further with the view to remove 'level' from the songplays table to avoid duplication.

## OPPORTUNITIES FOR IMPROVEMENT / UPDATES

<ol>
<li> Update the StageToRedshiftOperator to use the execution_date to only read in the relevant log file for processing </li>
<li> Update the quality checks being run </li>
<li> Add process to save the fact and dimension tables back into S3 for the analytics team to use. </li>
<li> Add a check to StageToRedshiftOperator so that if log files are missing for a specific date, it skips the remainder of the dag processing </li>
<li> Investigate how updates to missing data can be handled by the dag to automatically check for and include it in the processsed tables if found. i.e. If a log file from last week was added, it gets detected and processed in the next run of the dag. </li>  
</ol>
