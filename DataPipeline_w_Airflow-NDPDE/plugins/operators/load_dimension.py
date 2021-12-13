from os import truncate
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from sqlalchemy.orm import query

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate = """
        TRUNCATE {table}
    """

    insert = """
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
                 truncate_opt=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.target_table = target_table
        self.truncate_opt=truncate_opt

    def execute(self, context):
        self.log.info('LoadDimensionOperator WIP')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        
        redshift.run("""
            CREATE TABLE IF NOT EXISTS public.artists (
                artistid varchar(256) NOT NULL,
                name varchar(256),
                location varchar(256),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            );

            CREATE TABLE IF NOT EXISTS public.songs (
                songid varchar(256) NOT NULL,
                title varchar(256),
                artistid varchar(256),
                year int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );

            CREATE TABLE IF NOT EXISTS public.time (
                start_time timestamp NOT NULL,
                hour int4,
                day int4,
                week int4,
                month varchar(256),
                year int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            );

            CREATE TABLE IF NOT EXISTS public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                level varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );
        """)




        insert_formatted = LoadDimensionOperator.insert.format(
                table = self.target_table,
                sql_query = self.query
        )

        if self.truncate:
            truncate_formatted = LoadDimensionOperator.truncate.format(table = self.target_table)
            redshift.run(truncate_formatted)
            redshift.run(insert_formatted)
    
        else:
            redshift.run(insert_formatted)
