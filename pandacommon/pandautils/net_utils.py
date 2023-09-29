import copy
import os
import random
import socket

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.connection import allowed_gai_family

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from .thread_utils import MapWithLockAndTimeout

# DNS cache
dnsMap = MapWithLockAndTimeout()


# HTTP adaptor with randomized DNS resolution
class HTTPAdapterWithRandomDnsResolver(HTTPAdapter):
    # override to get connection to random host
    def get_connection(self, url, proxies=None):
        # parse URL
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port
        if port is None:
            if parsed.scheme == "http":
                port = 80
            else:
                port = 443
        # check record
        if parsed.hostname in dnsMap:
            dns_records = dnsMap[parsed.hostname]
        else:
            family = allowed_gai_family()
            dns_records = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
            dns_records = list(set([socket.getfqdn(record[4][0]) for record in dns_records]))
            dnsMap[parsed.hostname] = dns_records
        dns_records = copy.copy(dns_records)
        random.shuffle(dns_records)
        # loop over all hosts
        err = None
        for hostname in dns_records:
            addr = hostname
            if parsed.port is not None:
                addr += ":{0}".format(parsed.port)
            tmp_url = parsed._replace(netloc=addr).geturl()
            try:
                con = HTTPAdapter.get_connection(self, tmp_url, proxies=proxies)
                # return if valid
                if con is not None:
                    return con
            except Exception as e:
                err = e
        if err is not None:
            raise err
        return None


# utility function to get HTTPAdapterWithRandomDnsResolver
def get_http_adapter_with_random_dns_resolution():
    session = requests.Session()
    # no randomization if panda is behind real load balancer than DNS LB
    if "PANDA_BEHIND_REAL_LB" in os.environ:
        return session
    adapter = HTTPAdapterWithRandomDnsResolver(max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
