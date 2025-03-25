from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
            table = '',
            select_query = '',
            redshift_conn_id = '',
            appending_mode = True,
            *args,
            **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_query= select_query
        self.redshift_conn_id = redshift_conn_id
        self.appending_mode = appending_mode


    def execute(self, context):
        self.log.info('LoadDimensionOperator Task has Started')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.appending_mode == True:
            final_query = f""" INSERT INTO {self.table} 
            {self.select_query}
            """
        else:
            final_query = f""" 
            TRUNCATE TABLE {self.table};
            INSERT INTO {self.table} 
            {self.select_query}
            """

        self.log.info(f'Running {self.table} insertion')
        redshift.run(final_query)
