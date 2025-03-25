from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{acc_key}'
        SECRET_ACCESS_KEY '{sec_key}'
        JSON '{json_path}'
        """

    @apply_defaults
    def __init__(self,
                table = '',
                s3_key = '',
                redshift_conn_id = '',
                aws_credentials_id="",
                s3_bucket="",
                json_path='',
                *args, 
                **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.json_path = json_path


    def execute(self, context):
        self.log.info(f'Staging {self.table} task has started')

        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift Hook done")

        execution_date = context['execution_date']
        s3_key_formated = self.s3_key.format(**context)
        s3_path_formated = "s3://{}/{}".format(self.s3_bucket,s3_key_formated)

        self.log.info(f"The s3_path Formated is {s3_path_formated}")

        final_copy_query = StageToRedshiftOperator.copy_sql.format(
            table = self.table,
            s3_path = s3_path_formated,
            acc_key = aws_connection.login,
            sec_key = aws_connection.password,
            json_path = self.json_path )

        self.log.info(f"query {final_copy_query}")

        redshift.run(final_copy_query)

        self.log.info(f"Copying table {self.table} is Completed")







