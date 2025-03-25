from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,test_queries = [],redshift_conn_id='', *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.test_queries = test_queries
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('DataQualityOperator task has started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test_case in self.test_queries:
            test_query = test_case['query']
            expected_count = test_case['expected_count']
            query_result = redshift.get_records(test_query)
            query_count = query_result[0][0]
            if query_count != expected_count:
                raise ValueError(f"Data Quality Check Failed for that Query {test_query} | Result= {query_count} | Expected count= {expected_count}")
            else:
                self.log.info(f'Quality Check Passed for that Test Query {test_query}')

