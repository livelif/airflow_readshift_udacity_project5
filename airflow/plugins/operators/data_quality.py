from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    Check quality of dimension tables
    
    :param redshift_conn_id: Redshift connection ID
    :param test_query: Query to get data to used in the test
    :param expected_result: Expected result to the test
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_data_quality_list = "",
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_data_quality_list = sql_data_quality_list
    
    def execute(self, context):

        self.log.info("Getting credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running test")
        for sql in self.sql_data_quality_list:
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"UNIT TEST DATA QUALITY FAILED. {sql} RETURNED NO RESULTS")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"UNIT TEST DATA QUALITY FAILED. {sql} CONTAINED 0 ROWS")

            logging.info(f"{sql} WITH {records[0][0]} RECORDS")
    