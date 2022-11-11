from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info('Connecting to Redshift...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connected to Redshift')
        
        self.log.info('Load data from S3 to Redshift')
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql_query))
        