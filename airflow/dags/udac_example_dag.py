from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from helpers import CreateTableSql


default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('udac_example_dag_teste',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution_completo',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events_teste',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    region="us-west-2",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs_teste',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data/A/A/A/",
    region="us-west-2",
    dag=dag
)


load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table_teste',
    redshift_conn_id = 'redshift',
    table = 'songplays',
    select_sql = SqlQueries.songplay_table_insert,
    dag = dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    redshift_conn_id = 'redshift',
    table = 'users',
    select_sql = SqlQueries.user_table_insert,
    dag = dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_conn_id = 'redshift',
    table = 'songs',
    select_sql = SqlQueries.song_table_insert,
    dag = dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_conn_id = 'redshift',
    table = 'artists',
    select_sql = SqlQueries.artist_table_insert,
    dag = dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    redshift_conn_id = 'redshift',
    table = 'time',
    select_sql = SqlQueries.time_table_insert,
    dag = dag
)



sql_data_quality_list = ['SELECT count(1) from users where userid is null;', 'select count(1) from time where start_time is null;',
                         'select count(1) from artists where artistid is null;', 'select count(1) from songs where songid is null;']

run_quality = DataQualityOperator(
    task_id = 'Run_data_quality_songs',
    redshift_conn_id = 'redshift',
    sql_data_quality_list = sql_data_quality_list,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality
load_songplays_table >> load_song_dimension_table >> run_quality
load_songplays_table >> load_artist_dimension_table >> run_quality
load_songplays_table >> load_time_dimension_table >> run_quality
run_quality >> end_operator