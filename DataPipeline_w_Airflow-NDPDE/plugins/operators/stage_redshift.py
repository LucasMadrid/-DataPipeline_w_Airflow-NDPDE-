from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {table_destination}
        FROM '{s3_bucket}'
        ACCESS_KEY_ID '{access_id}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION 'us-west-2'
        JSON '{json_path}'
        COMPUPDATE OFF;
    """

    drop_sql = """
        DROP TABLE IF EXISTS {table}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path = '',
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.execution_date = kwargs.get('ds')
        
        
    def execute(self, context):
        self.log.info(f'StageToRedshiftOperator for {self.table}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Create {self.table} if it's not already created")
        
        # Run SQL CREATE statement for EVENTS and SONGS only if they not exists.

        drop_table_formatted = StageToRedshiftOperator.drop_sql.format(table = self.table)

        redshift.run(drop_table_formatted)
        
        redshift.run(
            """
            CREATE TABLE IF NOT EXISTS public.staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                level varchar(256),
                location varchar(256),
                method varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            );
            
            
            CREATE TABLE IF NOT EXISTS public.staging_songs (
                    num_songs int4,
                    artist_id varchar(256),
                    artist_name varchar(256),
                    artist_latitude numeric(18,0),
                    artist_longitude numeric(18,0),
                    artist_location varchar(256),
                    song_id varchar(256),
                    title varchar(256),
                    duration numeric(18,0),
                    year int4
                );

            """
        )   

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info(f'Copying data from{s3_path} to {self.table}...')
        copy_sql_formatted = StageToRedshiftOperator.copy_sql.format(
            table_destination = self.table,
            s3_bucket = s3_path,
            access_id = credentials.access_key,
            secret_key = credentials.secret_key,
            json_path = self.json_path,
        )

        redshift.run(copy_sql_formatted)
        