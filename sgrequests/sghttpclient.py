import ssl
import certifi
import urllib.parse
import base64
import json
import os
import http.client as http_client

class SgHttpClient:
    DEFAULT_PROXY_URL = "http://groups-RESIDENTIAL,country-us:{}@proxy.apify.com:8000/"

    def __init__(self, url):
        import ssl
        self.conn = self.get_conn(url)

    def get_conn(self, url):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_default_certs(purpose=ssl.Purpose.SERVER_AUTH)
        certs_path = certifi.where()
        context.load_verify_locations(cafile=certs_path)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        conn = None
        if 'PROXY_PASSWORD' in os.environ:
            proxy_password = os.environ["PROXY_PASSWORD"]
            proxy_url = os.environ["PROXY_URL"] if 'PROXY_URL' in os.environ else self.DEFAULT_PROXY_URL
            parsed_proxy_url = urllib.parse.urlsplit(proxy_url.format(proxy_password))
            proxy_port = parsed_proxy_url.port
            proxy_host = parsed_proxy_url.hostname
            conn = http_client.HTTPSConnection(proxy_host, proxy_port, context=context)
            username = parsed_proxy_url.username
            password = parsed_proxy_url.password
            auth_hash = base64.b64encode("{}:{}".format(username, password).encode("utf-8")).decode("utf-8")
            proxy_headers = {
                "Proxy-Authorization": "Basic {}".format(auth_hash)
            }
            conn.set_tunnel(url, headers=proxy_headers)
        else:
            conn = http_client.HTTPSConnection(url, context=context)
        conn.connect()
        return conn

    def get(self, path, **kwargs):
        self.conn.request("GET", path, **kwargs)
        return self.conn.getresponse().read()

    def post(self, path, json_data, **kwargs):
        self.conn.request("POST", path, json.dumps(json_data), **kwargs)
        return self.conn.getresponse().read()
