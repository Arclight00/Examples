from typing import Dict, Any
import requests


class Rest:
    def __init__(self):
        self.session = requests.session()

    def post(self, url: str, payload: Dict[str, Any]) -> requests.Response:
        """post http client
        :param url: string url
        :param payload: dictionary payload
        :return:
        """
        return self.session.post(url, json=payload)

    def get(self, url: str) -> requests.Response:
        """get http client
        :param url: string url
        :return:
        """
        return self.session.get(url)
