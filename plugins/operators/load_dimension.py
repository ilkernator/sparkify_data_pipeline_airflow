from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = "redshift",
                 sql_query = "",
                 destination_db = "public",
                 target_table = "",
                 tuncate_insert_mode = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.destination_db = destination_db
        self.target_table = target_table
        self.tuncate_insert_mode = tuncate_insert_mode
        self.truncate_sql_cmd = f"TRUNCATE TABLE {self.destination_db}.{self.target_table};"
        self.insert_into_sql_cmd = f"INSERT INTO {self.destination_db}.{self.target_table}"

    def execute(self, context):
        self.log.info('Running LoadDimensionOperator ...')
        self.log.info('Creating Postgres Hook ...')
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info(f'Running Query in {"Tuncate-Insert" if self.tuncate_insert_mode else "Insert-into"} Mode ...')
        redshift_hook.run(self.truncate_sql_cmd) if self.tuncate_insert_mode else None

        sql_cmd_formatted = self.sql_query.format(self.insert_into_sql_cmd)
        self.log.info(f'Formatted Query: \n {sql_cmd_formatted}')
        redshift_hook.run(sql_cmd_formatted)
