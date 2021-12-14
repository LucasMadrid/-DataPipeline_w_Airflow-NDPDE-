from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        self.log.info('DataQualityOperator executed')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failed_checks = 0
        failed_queries = []
        
        self.log.info('....Starting Data Quality Check....')
        for dq_check in self.dq_checks:
            
            sql_query = dq_check.get('check_sql')
            expected_result = dq_check.get('expected_result')
            
            records = redshift.get_records(sql_query)[0]
            
            if records[0] != expected_result:
                failed_checks+=1
                failed_queries.append(sql_query)
                
                
        if failed_checks > 0:
            self.log.info(f"{failed_checks} data quality checks failed.")
            self.log.info(failed_queries)
            raise ValueError('Data Quality Check failed.')
            
            
        self.log.info('Data Quality checks passed!')
            