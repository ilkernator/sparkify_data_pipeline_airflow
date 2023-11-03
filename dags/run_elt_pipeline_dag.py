from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from sql_cmnds.final_project_sql_statements import SqlQueries


DEFAULT_ARGS = {
    'owner': 'Ilker',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
REDSHIFT_CONN_ID = "redshift"
AWS_CONN_ID = "aws_credentials"
AWS_BUCKET = Variable.get('s3_bucket')
DESTINATION_DB = "public"


@dag(
    default_args=DEFAULT_ARGS,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def run_elt_pipeline():

    start_operator = EmptyOperator(task_id='Begin_execution')
    start_test_operator = EmptyOperator(task_id='start_test_operator')
    end_operator = EmptyOperator(task_id='Stop_execution')

    @task_group
    def staging_task_group():
        staging_tables_to_load = {"staging_events":["log-data","log_json_path.json"], "staging_songs":["song-data",'auto']}
        return [
            StageToRedshiftOperator(
                task_id=f"load_{table_name}",
                redshift_conn_id=REDSHIFT_CONN_ID,
                aws_credentials_id=AWS_CONN_ID,
                table=table_name,
                s3_bucket=AWS_BUCKET,
                s3_key=property_list[0],
                json_mode=property_list[1],
            ) for table_name, property_list in staging_tables_to_load.items()
        ]


    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id=REDSHIFT_CONN_ID,
        sql_query=SqlQueries.songplay_table_insert,
        destination_db=DESTINATION_DB,
        target_table="songplays",
        tuncate_insert_mode=False
    )


    @task_group
    def load_dimensions_task_group():
        dimenstion_tables_to_load = {
            "artists": SqlQueries.artist_table_insert, 
            "songs": SqlQueries.song_table_insert, 
            "time": SqlQueries.time_table_insert, 
            "users": SqlQueries.user_table_insert,
        }
        return [
            LoadDimensionOperator(
                task_id = f"load_{table_name}_dim_table",
                conn_id=REDSHIFT_CONN_ID,
                sql_query=insert_query,
                destination_db=DESTINATION_DB,
                target_table=table_name,
                tuncate_insert_mode=True,
            ) for table_name, insert_query in dimenstion_tables_to_load.items()
        ]

    @task_group
    def run_quality_check_task_group():
        tables_to_check = ["artists","songplays","songs","time","users","staging_events","staging_songs"]
        return [ 
                DataQualityOperator(
                task_id=f'run_data_quality_checks_on_{table_name}',
                redshift_conn_id=REDSHIFT_CONN_ID,
                target_table=table_name,
                target_db=DESTINATION_DB,
            ) for table_name in tables_to_check
        ]
    
    get_staging_task_group = staging_task_group()
    get_load_dimensions_task_group = load_dimensions_task_group()
    get_run_quality_check_task_group = run_quality_check_task_group()

    
    start_operator >> get_staging_task_group
    get_staging_task_group >> load_songplays_table 
    load_songplays_table >> get_load_dimensions_task_group 
    get_load_dimensions_task_group >> start_test_operator 
    start_test_operator >> get_run_quality_check_task_group
    get_run_quality_check_task_group >> end_operator 

run_elt_pipeline_project_dag = run_elt_pipeline()
    
