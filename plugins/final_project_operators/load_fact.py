from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                table = '',
                select_query = '',
                redshift_conn_id = '',
                 *args, 
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_query = select_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Starting Fact table task')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        inserting = f""" INSERT INTO {self.table} 
        {self.select_query}
        """
        self.log.info('Running Fact table insertion')
        redshift.run(inserting)

        self.log.info('Task LoadFactOperator is Completed successfully! ')
