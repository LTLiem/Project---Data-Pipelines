from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 update_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.update_mode = update_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.update_mode == "truncate":
            formatted_sql = "TRUNCATE TABLE {}".format(self.table)
            self.log.info("Truncating data in dimension table {}".format(self.table))
            redshift.run(formatted_sql)
            self.log.info("Data in table {} truncated".format(self.table))
        else:
            formatted_sql = "INSERT INTO {} {}".format(self.table, self.sql_query)
            self.log.info("Inserting data into dimension table {}".format(self.table))
            redshift.run(formatted_sql)
            self.log.info("Data inserted into table {} successfully".format(self.table))
                                                  
