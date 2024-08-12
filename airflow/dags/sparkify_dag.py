from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import StageToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'chunglm',
    'start_date': datetime(2024, 8, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          #schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# f= open(Variable.get('create_table_sql_path'))
# create_tables_sql = f.read()
# create_tables = PostgresOperator(
#     task_id='create_tables',
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=create_tables_sql
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="stg_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get("s3_log_prefix"),
    region="us-west-2",
    json_option=Variable.get('json_option_file')
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="stg_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get("s3_song_prefix"),
    region="us-west-2",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    truncate=True,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    truncate=True,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    truncate=True,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    truncate=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Data_quality_checks',
    dag=dag,
    checks=[
        { 'sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'result': 0 }, 
        { 'sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'result': 3 },
        { 'sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.users WHERE last_name IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set tasks ordering

#start_operator >> create_tables

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator