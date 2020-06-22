import urllib

from typing import Dict


class NashAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def append_token_to_url(self, url: str, params: Dict[str, any] = {}) -> str:
        param_str = urllib.parse.urlencode({"token": self.api_key})
        return f"{url}?{param_str}"
