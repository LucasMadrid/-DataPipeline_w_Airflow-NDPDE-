from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.tables=tables

    def execute(self, context):
        self.log.info('DataQualityOperator WIP')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:

            query_formatted = "SELECT COUNT(1) FROM {query_table}".format(query_table=table)
            records = redshift.get_records(query_formatted)

            if records is None or len(records[0]) < 1:
                raise ValueError(f"""
                    Data quality check failed at {table}. The table is not created or it has {len(records[0])} rows
                """)
        
            else:
                self.log.info(f"Data quality check on {table} passed. {len(records[0])} rows")

        self.log.info("All Data quality checks passed.")