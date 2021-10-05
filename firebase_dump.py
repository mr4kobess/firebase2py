import hashlib
import cfscrape
from os import name
import os
import requests
import json
import urllib
from pathlib import Path
from requests.api import get
from urllib.parse import urljoin
from loguru import logger

requests = cfscrape.create_scraper(delay=15)

class SubDom:
    BASE_URL = 'firebaseio.com'

    def __init__(self, sub_domain) -> None:
        self.sub_domain = sub_domain
        self.tables = dict()

    @property
    def is_valid(self):
        url = self._get_url('.json?shallow=true')
        r = requests.get(url, headers=self._get_headers(url))
        r_json = r.json()
        # print(r_json)
        if 'true' in r_json.values() or True in r_json.values():
            self.tables = r_json
            return True
        return False

    def _get_url(self, add_url):
        _url = f"https://{self.sub_domain}/{add_url}"
        return _url

    def _get_headers(self, url):
        return {
            'Accept': "application/json, text/plain, */*",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0',
            'Referer': url,
        }

    def dump(self):
        for name in (k for k, v in self.tables.items() if v == True):
            url = self._get_url(name + '.json')
            # print(url)
            with open(f'data/{self.sub_domain}/{name}.json', 'wb') as f:
                response = requests.get(url, stream=True, headers=self._get_headers(url))
                for chunk in response.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
                    f.flush()


if __name__ == '__main__':
    sd = SubDom(sub_domain='kodilan-d27d8.firebaseio.com')
    if sd.is_valid:

        os.makedirs('data/kodilan-d27d8.firebaseio.com/', exist_ok=True)
        sd.dump()
    
