from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    delete_sql_cmd = "DELETE FROM {}"
    copy_sql_cmd = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_mode = "",
                 backfill_year = "",
                 backfill_month = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.backfill_year = backfill_year
        self.backfill_month = backfill_month
        self.json_mode = json_mode


    def execute(self, context):
        self.log.info('Running StageToRedshiftOperator ...')
        self.log.info('Creating Hooks ...')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.backfill_year == "" or self.backfill_month == "":
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}" 
        else: 
            s3_path = f"s3://{self.s3_bucket}/{self.s3_key}/{self.backfill_year}/{self.backfill_month}"

        self.log.info('Deleting Data before copying ...')
        redshift_hook.run(self.delete_sql_cmd.format(self.table))

        json_mode_str = f"s3://{self.s3_bucket}/{self.json_mode}" if self.json_mode != "auto" else self.json_mode

        self.log.info(f'Copying Data from {s3_path} to {self.table} ...')
        formatted_sql_cmd = self.copy_sql_cmd.format(
            self.table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            json_mode_str
        )
        redshift_hook.run(formatted_sql_cmd)
