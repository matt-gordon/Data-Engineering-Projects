from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import DataQuality
from helpers import CleanData
from helpers import CreateTable

from datetime import datetime, timedelta

default_args = {
    'owner': 'Sparkify Pty Ltd.',
    'start_date': datetime.now(),
    'email_on_failure': False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'depends_on_past': False,
    'catchup_by_default': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load Sparkify data from S3 into Staging Tables and then create Fact & Dimension tables to support the Sparkify Analytics Team',
          schedule_interval='@hourly'
        )

       
start_operator = DummyOperator(task_id='Start_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    create_table = CreateTable.create_staging_events_table,
    table="public.staging_events",
    format = "json 's3://udacity-dend/log_json_path.json'",
    clean_data=True,
    cleaning_query=CleanData.clean_staging_events_table,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    create_table=CreateTable.create_staging_songs_table,
    table="public.staging_songs",
    format="json 'auto'",
    clean_data=True,
    cleaning_query=CleanData.clean_staging_songs_table,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id ="redshift",
    create_table = CreateTable.create_songplays_table,
    table ="public.factSongplays",
    load_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    create_table = CreateTable.create_user_dim_table,
    table = "public.dimUsers",
    load_query = SqlQueries.user_table_insert,
    truncate_mode = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    create_table = CreateTable.create_song_dim_table,
    table = "public.dimSongs",
    load_query = SqlQueries.song_table_insert,
    truncate_mode = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    create_table = CreateTable.create_artist_dim_table,
    table = "public.dimArtists",
    load_query = SqlQueries.artist_table_insert,
    truncate_mode = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    create_table = CreateTable.create_time_dim_table,
    table = "public.dimTime",
    load_query = SqlQueries.time_table_insert,
    truncate_mode = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    test_queries = DataQuality.test_queries,
    expected_results = DataQuality.test_results
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
