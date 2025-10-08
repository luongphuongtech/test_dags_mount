from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
# Cho phép insecure transport trong dev (nếu dùng http ở Keycloak)
# os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

class TrinoOAuth2Hook(BaseHook):
    """
    """
    def __init__(self, trino_conn_id='trino_default_oauth2'):
        super().__init__()
        self.trino_conn_id = trino_conn_id

    def get_conn(self):
        import requests
        import trino
        from trino.auth import JWTAuthentication
        from trino.auth import BasicAuthentication
        import ssl
        conn = self.get_connection(self.trino_conn_id)
        extras = conn.extra_dejson


        token_url = extras.get("token_url","http://keycloak.data-service.svc.cluster.local:8070/realms/trino-realm/protocol/openid-connect/token")
        client_id = conn.login
        client_secret = conn.password
        scope = extras.get("scope", "openid")  # default scope is 'openid'

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

        
        host = conn.host
        port = conn.port or 8080  
        catalog = extras.get("catalog", "tpch")
        schema = extras.get("schema", "information_schema")

        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        user = "admin"
        password = "12345678"
     
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            http_scheme="https",  # hoặc "http" nếu môi trường dev
            auth=BasicAuthentication(user, password),
            catalog=catalog,
            schema=schema,
            http_headers={},
            verify=False
        )

        return conn
    
class TrinoOAuth2HookPlugin(AirflowPlugin):
    name = "trino_oauth2_hook"
    hooks = [TrinoOAuth2Hook]
