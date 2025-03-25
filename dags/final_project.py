from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from common import final_project_sql_statements


default_args = {
        'owner': 'ragab',
        'start_date': pendulum.now(),
        'depends_on_past' : False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'catchup': False,
        'email_on_retry': False }

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():
    """
    - Kindly Make Sure to create the tables first using the create statments in create_tables.sql
    - Kindly Uncomment the other s3_key for stage_events task if you want to run on specific execution date.
    """
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_key =  "log-data/",  ## use this if you want to load all data without a specific execution date
        #s3_key =  "log-data/{{ execution_date.strftime('%Y/%m') }}/{{ execution_date.strftime('%Y-%m-%d') }}-events.json", ## used to run on a specific execution date
        redshift_conn_id = 'redshift',
        aws_credentials_id="aws_credentials",
        s3_bucket="ragab-af",
        json_path='s3://ragab-af/log_json_path.json'
    )
       
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_key = "song-data/A/A/A/", ## just using a sample of song-data to test because it's so big
        redshift_conn_id = 'redshift',
        aws_credentials_id="aws_credentials",
        s3_bucket="ragab-af",
        json_path='auto' ## same as the dwh project of course 2
    )

    load_songplays_table = LoadFactOperator(
            task_id='Load_songplays_fact_table',
            table = 'songplays',
            select_query = final_project_sql_statements.SqlQueries.songplay_table_insert,
            redshift_conn_id = 'redshift'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table = 'users',
        select_query = final_project_sql_statements.SqlQueries.user_table_insert,
        redshift_conn_id = 'redshift',
        appending_mode = False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table = 'songs',
        select_query = final_project_sql_statements.SqlQueries.song_table_insert,
        redshift_conn_id = 'redshift',
        appending_mode = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            table = 'artists',
            select_query = final_project_sql_statements.SqlQueries.artist_table_insert,
            redshift_conn_id = 'redshift',
            appending_mode = False
    )

    load_time_dimension_table = LoadDimensionOperator(
            task_id='Load_time_dim_table',
            table = 'time',
            select_query = final_project_sql_statements.SqlQueries.time_table_insert,
            redshift_conn_id = 'redshift',
            appending_mode = False
    )

    ## add your test cases in below list as dict format {'query':'','expected_count': 0 }
    test_cases = [
        {'query': 'SELECT count(*) FROM time','expected_count': 6820 }
    ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = 'redshift',
        test_queries = test_cases
    )

    end_operator = DummyOperator(task_id='Stop_execution')




    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_song_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks >> end_operator
    load_songplays_table >> load_time_dimension_table >> run_quality_checks >> end_operator




final_project_dag = final_project()