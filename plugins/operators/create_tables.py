from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self, redshift_conn_id, *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        self.log.info('Initiating Postgres Hook.')
        redshift = PostgresHook(redshift_conn_id = self.redshift_conn_id)
        
        self.log.info('Read query and start creating tables.')
                
        with open('/home/workspace/airflow/create_tables.sql', 'r') as file:
            sql_queries = file.read()
            
        redshift.run(sql_queries)
        self.log.info("All tables created in Redshift.")
