from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.sql.expression import table

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    delete = """
        DELETE FROM {table}
    """

    sql_query = """
        INSERT INTO {table}
        {sql_query}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 query='',
                 target_table='',
                 append_only=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.target_table = target_table
        self.append_only = append_only

    def execute(self, context):
        self.log.info(f'LoadFactOperator for {self.target_table}')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f'Running create table statement if it is not already created.')
        redshift.run("""
            CREATE TABLE IF NOT EXISTS public.songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                level varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );
        """
        )
        
        query_formatted = LoadFactOperator.sql_query.format(
            table = self.target_table,
            sql_query = self.query
        )

        if not self.append_only:
            delete_formatted = LoadFactOperator.delete.format(table=self.target_table)

            redshift.run(delete_formatted)
            redshift.run(query_formatted)
            
        else:
            redshift.run(query_formatted)
