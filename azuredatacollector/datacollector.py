"""Azure Monitor Data Collector API Client

Raises:
    DataCollectorError: Returns errors if API request was unsuccessful

Returns:
    DataCollectorClient: Data Collector API client
"""
import base64
import datetime
import hashlib
import hmac
import json
from typing import Dict

from requests import Session

BASE_REQUEST_URI = "https://{}.ods.opinsights.azure.com/{}?api-version={}"
DEFAULT_RESOURCE = "/api/logs"
DEFAULT_API_VERSION = "2016-04-01"
DEFAULT_CONTENT_TYPE = "application/json"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_BATCH_SIZE = 29000000


class DataCollectorError(Exception):
    """Exception for all error returned by the Data Collector API"""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class DataCollectorClient:
    """Log Analytics Data Collector API Client"""

    def __init__(
        self,
        customer_id: str,
        shared_key: str,
    ):
        self.customer_id = customer_id
        self.shared_key = shared_key
        self.request_uri = BASE_REQUEST_URI.format(
            self.customer_id, DEFAULT_RESOURCE, DEFAULT_API_VERSION
        )

        self._timeout: int = DEFAULT_TIMEOUT
        self._max_batch_size: int = DEFAULT_MAX_BATCH_SIZE
        self._proxies: Dict[str, str] = {}

    def __get_xmsdate_str(self, x_ms_date: datetime.datetime) -> str:
        return x_ms_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

    def __build_authorization_headers(
        self,
        content_length: int,
        log_type: str,
        x_ms_date: datetime.datetime,
    ) -> dict:
        """Builds authorization header as Data Collector API requirement

        Args:
            content_length (int): string length of data to be uploaded
            log_type (str): destination table name
            x_ms_date (datetime): datetime. Default is UTC now
        Returns:
            dict: headers with authorization signature
        """

        headers = {}

        x_ms_date_str = self.__get_xmsdate_str(x_ms_date)
        x_headers = "x-ms-date:" + x_ms_date_str

        string_to_hash = f"POST\n{content_length}\n{DEFAULT_CONTENT_TYPE}\n{x_headers}\n{DEFAULT_RESOURCE}"

        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        authorization = f"SharedKey {self.customer_id}:{encoded_hash}"

        headers["content-type"] = DEFAULT_CONTENT_TYPE
        headers["Authorization"] = authorization
        headers["Log-Type"] = log_type
        headers["x-ms-date"] = x_ms_date_str

        return headers

    def __batch(self, data: list) -> list:
        """Divide rows into batches to ensure total request size stays below the 30MB limit.

        Args:
            data (list): data to be divided into batches

        Returns:
            list: list of batched rows (list of lists)
        """
        batches: list = []
        tmp: list = []
        batch_size: int = 0

        for row in data:
            row_size = len(str(row))
            if batch_size + row_size <= self._max_batch_size:
                tmp.append(row)
                batch_size += row_size
            else:
                batches.append(tmp)
                tmp = [row]
                batch_size = row_size

        if batch_size:
            batches.append(tmp)

        return batches

    @property
    def timeout(self) -> int:
        """Sets API call timeout. Default is 30 seconds.

        Returns:
            int: seconds
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value: int):
        self._timeout = value

    @property
    def max_batch_size(self) -> int:
        """Sets maximum data batch size based on string length. Default
        is 29000000 to allow some overhead for the request headers while
        keeping the total post size below 30MB.

        Returns:
            int: batch size
        """
        return self._max_batch_size

    @max_batch_size.setter
    def max_batch_size(self, value: int):
        self._max_batch_size = value

    @property
    def proxies(self) -> dict:
        """Sets API call via proxy. Default is to not use any proxy.

        Returns:
            dict: proxy
        """
        return self._proxies

    @proxies.setter
    def proxies(self, value: Dict[str, str]):
        self._proxies = value

    def post_data(self, data: list, log_type: str) -> list[int]:
        """Post data to the Data Collector API

        Args:
            data (list): list (rows) of data
            log_type (str): destination table name

        Returns:
            list: metric with number of rows uploaded per batch
        """

        batched_data = self.__batch(data)
        metric = []

        with Session() as session:

            session.proxies = self.proxies
            for batch in batched_data:

                data_batch = json.dumps(batch)

                headers = self.__build_authorization_headers(
                    content_length=len(data_batch),
                    log_type=log_type,
                    x_ms_date=datetime.datetime.utcnow(),
                )

                session.headers = headers

                response = session.post(
                    data=data_batch, url=self.request_uri, timeout=self.timeout
                )

                if response.status_code != 200:
                    raise DataCollectorError(
                        f"Error uploading, status code: {response.status_code}. {response.text}"
                    )

                metric.append(len(batch))

        return metric
