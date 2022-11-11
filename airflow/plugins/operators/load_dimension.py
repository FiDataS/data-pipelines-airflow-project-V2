from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_query=sql_query
        self.append=append

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        self.log.info('Connect to Redshift...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connected to Redshift')
        
        if self.append == False:
            self.log.info(f"Delete table {self.table}...")
            redshift.run(f"DELETE FROM {self.table}")
            self.log.info(f"Deleted table {self.table}")
                       
        self.log.info(f"Load data from S3 to Redshift into Dimension Table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        self.log.info(f"Table {self.table} successfully loaded into Redshift.")
