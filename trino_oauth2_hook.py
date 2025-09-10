from airflow.hooks.base import BaseHook
import requests
import trino
from trino.auth import JWTAuthentication
import ssl
import os

# Cho phép insecure transport trong dev (nếu dùng http ở Keycloak)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

class TrinoOAuth2Hook(BaseHook):
    """
    Custom Hook cho Trino + OAuth2 sử dụng JWTAuthentication
    """

    conn_name_attr = 'trino_conn_id'
    default_conn_name = 'trino_default_oauth2'
    conn_type = 'trino_oauth2'
    hook_name = 'Trino with OAuth2'

    def __init__(self, trino_conn_id='trino_default_oauth2'):
        super().__init__()
        self.trino_conn_id = trino_conn_id

    def get_conn(self):
        # Lấy thông tin kết nối từ Airflow connection
        conn = self.get_connection(self.trino_conn_id)
        extras = conn.extra_dejson

        # Cấu hình OAuth2
        token_url = extras.get("token_url")
        client_id = conn.login
        client_secret = conn.password
        scope = extras.get("scope", "openid")  # default scope is 'openid'

        # Lấy access token từ Keycloak
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "scope": scope
            },
            auth=(client_id, client_secret)
        )
        token = resp.json()
        access_token = token["access_token"]
        self.log.info("Access token obtained: %s", access_token)

        # Cấu hình Trino
        host = conn.host
        port = conn.port or 8443  # Sử dụng cổng mặc định nếu không có
        catalog = extras.get("catalog", "hive")
        schema = extras.get("schema", "default")

        # SSL context để vô hiệu hóa xác thực hostname và certificate trong môi trường dev
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Kết nối tới Trino
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            http_scheme="http",  # Nếu bạn đang sử dụng https
            auth=JWTAuthentication(access_token),
            catalog=catalog,
            schema=schema,
            http_headers={},
            verify=False  # Hoặc có thể bật xác thực SSL nếu cần
        )

        return conn
