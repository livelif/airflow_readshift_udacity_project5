from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
    Load dimension tables
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Table name in REDSHIFT
    :param select_sql: Query to get data from stage tables
    :param create_table_query: Query used to create table in REDSHIFT
    :param upsert: Boolean to ALLOW UPSERT opration in dimensions
    :param primary_key: Primary key used in UPSERT operation
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 select_sql = "",
                 truncate_table = False,
                 primary_key = "",
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate_table = truncate_table
        self.primary_key = primary_key
    
    def execute(self, context):
        
        self.log.info("Getting credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Deleting from {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
       
  
        self.log.info("Inserting/Appending data in the table")
        table_insert_sql = f"""
                            insert into {self.table}
                            {self.select_sql}
                        """
        redshift.run(table_insert_sql)
