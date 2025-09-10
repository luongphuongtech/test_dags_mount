from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from trino_oauth2_hook import TrinoOAuth2Hook


class TrinoOAuth2Operator(BaseOperator):
    """
    Operator để chạy SQL trên Trino sử dụng OAuth2
    """

    @apply_defaults
    def __init__(self, sql, trino_conn_id="trino_default_oauth2", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.trino_conn_id = trino_conn_id

    def execute(self, context):
        hook = TrinoOAuth2Hook(trino_conn_id=self.trino_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        rows = cursor.fetchall()
        self.log.info("Query result: %s", rows)
        return rows
