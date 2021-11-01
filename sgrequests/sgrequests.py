import logging
import os
import re
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Any, Coroutine, Callable, Union, Set

import httpx
from httpcore._sync import base as sync_base
from httpcore._async import base as async_base
from httpx import Timeout, RequestError, codes, Response, HTTPError, Request, HTTPStatusError
from httpx._client import BaseClient
from httpx._types import RequestData, QueryParamTypes, HeaderTypes, CookieTypes, RequestFiles, VerifyTypes
from sglogging import SgLogSetup
from tenacity import stop_after_attempt, retry, retry_if_exception_type, before_sleep_log, wait_incrementing

import random

@dataclass(frozen=True)
class SgRequestError(Exception):
    """
    Communicates all errors, alongside with the context in which the error was made.
    """
    request: Optional[Request] = None
    response: Optional[Response] = None
    status_code: Optional[int] = response.status_code if response else None
    message: Optional[str] = None
    base_exception: Optional[Exception] = None

    def __str__(self):
        return f"Msg: {self.message}, " \
               f"Status: {self.status_code}, " \
               f"Base Exception: {self.base_exception}"


@dataclass(frozen=True)
class CriticalSgRequestError(Exception):
    base_exception: SgRequestError


class SgRequestsBase:
    DEFAULT_RETRIES = 10
    DEFAULT_IP_ROTATION_RETRIES_BEHIND_PROXY = 10
    DEFAULT_TIMEOUT = Timeout(timeout=61, connect=61)
    DEFAULT_PROXY_URL = "http://groups-RESIDENTIAL,country-us:{}@proxy.apify.com:8000/"
    DEFAULT_DONT_RETRY_STATUS_CODES = frozenset(range(400, 600))

    STORM_PROXIES = [
        "http://5.79.73.131:13080",
    ]

    _IP_ROTATION_MAX_RETRIES = 10
    __BANNED_IP_SET = set()
    _CONNECTION_RETRIES = 10
    __instance_id = 0
    _ip_refresh_lock = Lock()

    _main_logger = SgLogSetup().get_logger(logger_name='SgRequests_main_log')

    def __init__(self,
                 proxy_country: Optional[str] = None,
                 dont_retry_status_codes: Set[int] = DEFAULT_DONT_RETRY_STATUS_CODES,
                 dont_retry_status_codes_exceptions: Set[int] = frozenset()):
        """
        Asynchronous SgRequests, backed by httpx, with proxy rotation and lots of customization.

        :param proxy_country: [None] Optionally, override the default proxy 2-letter country code
                              (`us` at the time of writing).
        :param dont_retry_status_codes: [SgRequestsAsync.DEFAULT_DONT_RETRY_STATUS_CODES] Skip retries for these status codes.
        :param dont_retry_status_codes_exceptions: Exceptions to `dont_retry_status_codes`. Defaults to an empty set.
        """
        self.__behind_proxy: bool = True
        self.__proxy_country = proxy_country
        self.__dont_retry_status_codes = set.difference(set(dont_retry_status_codes),
                                                        set(dont_retry_status_codes_exceptions))

        self._session: Union[httpx.AsyncClient, httpx.Client, None] = None
        self.__ip = None
        SgRequestsBase.__instance_id += 1
        self._log = SgLogSetup().get_logger(logger_name=f'sgrequests{SgRequestsBase.__instance_id}')

    def _behind_proxy(self) -> bool:
        return self.__behind_proxy

    def _client(self) -> Union[httpx.AsyncClient, httpx.Client, None]:
        return self._session

    # custom
    def _get_random_proxy(self) -> str:
        return random.choice(self.STORM_PROXIES)

    def _mk_proxy_url(self) -> str:
        return self._get_random_proxy()

        proxy_password = os.environ["PROXY_PASSWORD"]
        url = os.environ["PROXY_URL"] if 'PROXY_URL' in os.environ else self.DEFAULT_PROXY_URL
        if self.__proxy_country:
            url = re.sub(r"country-([a-zA-Z]{2}):", f"country-{self.__proxy_country}:", url)

        self._log.info(f"Set proxy address to: {url}")

        proxy_url = url.format(proxy_password)
        return proxy_url

    def _get_public_ip(self) -> Optional[str]:
        try:
            return httpx.get('https://jsonip.com/').json()['ip']
        except:
            # TODO - try another service; e.g. "ipify"
            self._log.warning('Non-critical: Unable to fetch public IP. Proceeding without.')
            return None

    def _analyze_resp_status(self, response: Response) -> Response:
        if response.status_code in self.__dont_retry_status_codes:
            raise CriticalSgRequestError(
                    SgRequestError(request=response.request,  # we're not catching this one here
                                   response=response,
                                   status_code=response.status_code,
                                   message=f'Response status code [{response.status_code}] should immediately fail.'))

        response.raise_for_status()  # make sure to raise for an error status_code, so we can examine it later
        if codes.is_redirect(response.status_code):
            err_msg = 'Failed to follow redirects; Giving up.'
            raise SgRequestError(request=response.request,  # we're not catching this one here
                                 response=response,
                                 message=err_msg)
        else:
            return response

    def _refresh_ip(self):
        with SgRequestsBase._ip_refresh_lock:
            self.__ip = self.__get_fresh_ip_recur()

    def __get_fresh_ip_recur(self, retries=0):
        public_ip = self._get_public_ip()
        is_banned = public_ip in SgRequestsBase.__BANNED_IP_SET
        if is_banned and retries < SgRequestsBase._IP_ROTATION_MAX_RETRIES:
            return self.__get_fresh_ip_recur(retries=retries+1)
        else:
            if is_banned:
                self._log.info(f'Exhausted all retries; proceeding with banned IP [{public_ip}]')
            else:
                if public_ip:
                    self._log.info(f'Refreshed public IP [{public_ip}]')
                else:
                    self._log.info('Refreshed public IP [< unable to determine >]')
            return public_ip

    def _ban_ip(self):
        with SgRequestsBase._ip_refresh_lock:
            if self.__ip:
                self._log.info(f"Banning IP address: {self.__ip}")
                self.__BANNED_IP_SET.add(self.__ip)

    @staticmethod
    def raise_on_err(result: Union[Response, SgRequestError]) -> Response:
        """
        Converts the Union[Response, SgRequestError] result semantics into a:
        > Return successful Response
        > Raise on any failure

        So instead of:
        result = http.get(...)
        if isinstance(result, Response):
          # happy path
        else:
          # error handling

        You can do:

        try:
          result = SgRequests.return_or_throw(http.get(...))
          # happy path
        except SgRequestError as e:
          # error handling

        """
        if isinstance(result, Response):
            return result
        elif isinstance(result, SgRequestError):
            raise result
        else:
            raise Exception(f"Unexpected result: {result}")


class SgRequestsAsync(SgRequestsBase):

    def __init__(self,
                 proxy_country: Optional[str] = None,
                 dont_retry_status_codes: Set[int] = SgRequestsBase.DEFAULT_DONT_RETRY_STATUS_CODES,
                 dont_retry_status_codes_exceptions: Set[int] = frozenset(),
                 timeout_config: Timeout = SgRequestsBase.DEFAULT_TIMEOUT,
                 retries_with_fresh_proxy_ip: int = SgRequestsBase.DEFAULT_IP_ROTATION_RETRIES_BEHIND_PROXY,
                 verify_ssl: VerifyTypes = True):
        """
        Asynchronous SgRequests, backed by httpx, with proxy rotation and lots of customization.

        :param proxy_country: [None] Optionally, override the default proxy 2-letter country code
                              (`us` at the time of writing).
        :param dont_retry_status_codes: [SgRequestsAsync.DEFAULT_DONT_RETRY_STATUS_CODES] Skip retries for these status codes.
        :param dont_retry_status_codes_exceptions: Exceptions to `dont_retry_status_codes`. Defaults to an empty set.
        :param timeout_config: [SgRequestsAsync.DEFAULT_TIMEOUT] HTTP timeout configuration. See `httpx`'s Timeout object.
        :param retries_with_fresh_proxy_ip: How many times to rotate proxy IPs on errors, for each request errors before giving up?
        :param verify_ssl: See [https://www.python-httpx.org/advanced/] for the `SSL certificates` section
        """
        self.__timeout = timeout_config
        self.__retries_with_proxy_rotation = retries_with_fresh_proxy_ip
        self.__verify_ssl = verify_ssl

        super(SgRequestsAsync, self).__init__(proxy_country=proxy_country,
                                              dont_retry_status_codes=dont_retry_status_codes,
                                              dont_retry_status_codes_exceptions=dont_retry_status_codes_exceptions)

    async def __aenter__(self):
        await self.__refresh_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__aclose()

    async def __refresh_client(self):
        await self.__aclose()
        proxy_url = None
        if self._behind_proxy():
            self._refresh_ip()
            proxy_url = self._mk_proxy_url()

        self._session = self._mk_client(proxy_url=proxy_url)

    async def __execute_http_async(self,
                                   cmd: Callable[[], Coroutine[Any, Any, Response]],
                                   proxy_retries=0) -> Union[Response, SgRequestError]:
        try:
            return await self.__retry_request_async(cmd)
        except Exception as e:
            result = await self.__interpret_resp_errors(e=e, proxy_retries=proxy_retries)
            if isinstance(result, bool):
                return await self.__execute_http_async(cmd, proxy_retries=proxy_retries+1)
            else:
                return result

    @retry(retry=(retry_if_exception_type(RequestError) | retry_if_exception_type(HTTPStatusError) | retry_if_exception_type(async_base.NewConnectionRequired)),
           reraise=True,
           stop=stop_after_attempt(SgRequestsBase.DEFAULT_RETRIES),
           wait=wait_incrementing(start=1, increment=3, max=31),
           before_sleep=before_sleep_log(SgRequestsBase._main_logger, logging.WARNING))
    async def __retry_request_async(self, cmd: Callable[[], Coroutine[Any, Any, Response]]) -> Response:
        response = await cmd()
        return self._analyze_resp_status(response)

    async def __interpret_resp_errors(self, e: Exception, proxy_retries: int) -> Union[Response, SgRequestError, bool]:
        if isinstance(e, HTTPStatusError):
            request, response = e.request, e.response
            status_code = response.status_code
            if self._behind_proxy():
                self._ban_ip()

                if proxy_retries <= self.__retries_with_proxy_rotation:
                    self._log.info(f"Request failed with status: [{response.status_code}]. "
                                    "Rotating IPs to rule out IP blocking.")
                    await self.__refresh_client()
                    return True
                else:
                    err_msg = f'Retries exhausted [{self.__retries_with_proxy_rotation}]; giving up.'
                    self._log.warning(err_msg)
                    return SgRequestError(request=request,
                                          response=response,
                                          message=err_msg,
                                          status_code=status_code)
            else:
                return SgRequestError(request=response.request,
                                      response=response,
                                      message=f"Response received with error status-code: [{status_code}]",
                                      status_code=status_code)

        elif isinstance(e, HTTPError):
            return SgRequestError(request=None,
                                  response=None,
                                  message=f"Unexpected HTTP error occurred; check `base_exception` for details.",
                                  base_exception=e)
        elif isinstance(e, SgRequestError):
            return e
        elif isinstance(e, CriticalSgRequestError):
            return e.base_exception
        elif isinstance(e, Exception):
            self._log.error(f'Unexpected error', exc_info=e)
            return SgRequestError(request=None,
                                  response=None,
                                  message=f"Unexpected error occurred; check `base_exception` for details.",
                                  base_exception=e)

    async def request(self,
                      url: str,
                      method: str = 'GET',
                      data: Optional[RequestData] = None,
                      params: Optional[QueryParamTypes] = None,
                      headers: Optional[HeaderTypes] = None,
                      cookies: Optional[CookieTypes] = None,
                      files: Optional[RequestFiles] = None,
                      json: Any = None) -> Union[Response, SgRequestError]:
        """
        A near-complete subset of httpx.request(...).
        Throws an `SgRequestError` in the case the result isn't in the 2XX range.
        """
        return await self.__execute_http_async(
            lambda: self._client().request(method=method,
                                           url=url,
                                           data=data,
                                           params=params,
                                           headers=headers,
                                           cookies=cookies,
                                           files=files,
                                           json=json,
                                           allow_redirects=True))

    async def get(self,
                  url: str,
                  data: Optional[RequestData] = None,
                  params: Optional[QueryParamTypes] = None,
                  headers: Optional[HeaderTypes] = None,
                  cookies: Optional[CookieTypes] = None,
                  files: Optional[RequestFiles] = None,
                  json: Any = None) -> Union[Response, SgRequestError]:
        return await self.request(url=url,
                                  method='GET',
                                  data=data,
                                  params=params,
                                  headers=headers,
                                  cookies=cookies,
                                  files=files,
                                  json=json)

    async def post(self,
                   url: str,
                   data: Optional[RequestData] = None,
                   params: Optional[QueryParamTypes] = None,
                   headers: Optional[HeaderTypes] = None,
                   cookies: Optional[CookieTypes] = None,
                   files: Optional[RequestFiles] = None,
                   json: Any = None) -> Union[Response, SgRequestError]:
        return await self.request(url=url,
                                  method='POST',
                                  data=data,
                                  params=params,
                                  headers=headers,
                                  cookies=cookies,
                                  files=files,
                                  json=json)

    async def put(self,
                  url: str,
                  data: Optional[RequestData] = None,
                  params: Optional[QueryParamTypes] = None,
                  headers: Optional[HeaderTypes] = None,
                  cookies: Optional[CookieTypes] = None,
                  files: Optional[RequestFiles] = None,
                  json: Any = None) -> Union[Response, SgRequestError]:
        return await self.request(url=url,
                                  method='PUT',
                                  data=data,
                                  params=params,
                                  headers=headers,
                                  cookies=cookies,
                                  files=files,
                                  json=json)

    async def head(self,
                   url: str,
                   data: Optional[RequestData] = None,
                   params: Optional[QueryParamTypes] = None,
                   headers: Optional[HeaderTypes] = None,
                   cookies: Optional[CookieTypes] = None,
                   files: Optional[RequestFiles] = None,
                   json: Any = None) -> Union[Response, SgRequestError]:
        return await self.request(url=url,
                                  method='HEAD',
                                  data=data,
                                  params=params,
                                  headers=headers,
                                  cookies=cookies,
                                  files=files,
                                  json=json)

    async def delete(self,
                     url: str,
                     data: Optional[RequestData] = None,
                     params: Optional[QueryParamTypes] = None,
                     headers: Optional[HeaderTypes] = None,
                     cookies: Optional[CookieTypes] = None,
                     files: Optional[RequestFiles] = None,
                     json: Any = None) -> Union[Response, SgRequestError]:
        return await self.request(url=url,
                                  method='DELETE',
                                  data=data,
                                  params=params,
                                  headers=headers,
                                  cookies=cookies,
                                  files=files,
                                  json=json)

    async def set_proxy_url(self, proxy_url: str):
        os.environ.update({'PROXY_URL': proxy_url})
        await self.__refresh_client()

    def _mk_client(self, proxy_url: Optional[str]) -> BaseClient:
        return httpx.AsyncClient(proxies=proxy_url,
                                 trust_env=False,
                                 timeout=self.__timeout,
                                 verify=self.__verify_ssl,
                                 transport=httpx.AsyncHTTPTransport(retries=SgRequestsBase._CONNECTION_RETRIES,
                                                                    verify=self.__verify_ssl,
                                                                    trust_env=False,
                                                                    http2=True)
                                 )

    async def __aclose(self):
        session = self._client()
        if session:
            await session.aclose()

    def clear_cookies(self):
        if self._session:
            self._session.cookies.clear()

    def my_public_ip(self) -> Optional[str]:
        """
        This is set if we are behind an Apify proxy.
        Failure to retrieve the public IP will result in it being None.
        """
        return self.__ip


class SgRequests(SgRequestsBase):

    def __init__(self,
                 proxy_country: Optional[str] = None,
                 dont_retry_status_codes: Set[int] = SgRequestsBase.DEFAULT_DONT_RETRY_STATUS_CODES,
                 dont_retry_status_codes_exceptions: Set[int] = frozenset(),
                 timeout_config: Timeout = SgRequestsBase.DEFAULT_TIMEOUT,
                 retries_with_fresh_proxy_ip: int = 4,
                 verify_ssl: VerifyTypes = True):
        """
        Synchronous SgRequests, backed by httpx, with proxy rotation and lots of customization.

        :param proxy_country: [None] Optionally, override the default proxy 2-letter country code
                              (`us` at the time of writing).
        :param dont_retry_status_codes: [SgRequestsAsync.DEFAULT_DONT_RETRY_STATUS_CODES] Skip retries for these status codes.
        :param dont_retry_status_codes_exceptions: Exceptions to `dont_retry_status_codes`. Defaults to an empty set.
        :param timeout_config: [SgRequests.DEFAULT_TIMEOUT] HTTP timeout configuration. See `httpx`'s Timeout object.
        :param retries_with_fresh_proxy_ip: [4] How many times to rotate proxy IPs on errors,
                                                for each request errors before giving up?
        :param verify_ssl: See [https://www.python-httpx.org/advanced/] for the `SSL certificates` section
        """
        self.__timeout = timeout_config
        self.__retries_with_proxy_rotation = retries_with_fresh_proxy_ip
        self.__verify_ssl = verify_ssl

        super().__init__(proxy_country=proxy_country,
                         dont_retry_status_codes=dont_retry_status_codes,
                         dont_retry_status_codes_exceptions=dont_retry_status_codes_exceptions)
        self.__refresh_client()

    def __refresh_client(self):
        self.close()
        proxy_url = None
        if self._behind_proxy():
            self._refresh_ip()
            proxy_url = self._mk_proxy_url()
        self._session = self._mk_client(proxy_url=proxy_url)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self._client():
            self._client().close()

    def __execute_http_sync(self,
                            cmd: Callable[[], Response],
                            proxy_retries=0) -> Union[Response, SgRequestError]:
        try:
            return self.__retry_request_sync(cmd)
        except Exception as e:
            result = self.__interpret_resp_errors(e=e, proxy_retries=proxy_retries)
            if isinstance(result, bool):
                return self.__execute_http_sync(cmd, proxy_retries=proxy_retries + 1)
            else:
                return result

    def __interpret_resp_errors(self, e: Exception, proxy_retries: int) -> Union[Response, SgRequestError, bool]:
        if isinstance(e, HTTPStatusError):
            request, response = e.request, e.response
            status_code = response.status_code
            if self._behind_proxy():
                self._ban_ip()

                if proxy_retries <= self.__retries_with_proxy_rotation:
                    self._log.info(f"Request failed with status: [{response.status_code}]. "
                                    "Rotating IPs to rule out IP blocking.")
                    self.__refresh_client()
                    return True
                else:
                    err_msg = f'Retries exhausted [{self.__retries_with_proxy_rotation}]; giving up.'
                    self._log.warning(err_msg)
                    return SgRequestError(request=request,
                                          response=response,
                                          message=err_msg,
                                          status_code=status_code)
            else:
                return SgRequestError(request=response.request,
                                      response=response,
                                      message=f"Response received with error status-code: [{status_code}]",
                                      status_code=status_code)

        elif isinstance(e, HTTPError):
            return SgRequestError(request=None,
                                  response=None,
                                  message=f"Unexpected HTTP error occurred; check `base_exception` for details.",
                                  base_exception=e)
        elif isinstance(e, SgRequestError):
            return e
        elif isinstance(e, CriticalSgRequestError):
            return e.base_exception
        elif isinstance(e, Exception):
            self._log.error(f'Unexpected error', exc_info=e)
            return SgRequestError(request=None,
                                  response=None,
                                  message=f"Unexpected error occurred; check `base_exception` for details.",
                                  base_exception=e)

    @retry(retry=(retry_if_exception_type(RequestError) | retry_if_exception_type(HTTPStatusError) | retry_if_exception_type(sync_base.NewConnectionRequired)),
           reraise=True,
           stop=stop_after_attempt(SgRequestsBase.DEFAULT_RETRIES),
           wait=wait_incrementing(start=1, increment=3, max=31),
           before_sleep=before_sleep_log(SgRequestsBase._main_logger, logging.WARNING))
    def __retry_request_sync(self, cmd: Callable[[], Response]) -> Response:
        return self._analyze_resp_status(cmd())

    def request(self,
                url: str,
                method: str = 'GET',
                data: Optional[RequestData] = None,
                params: Optional[QueryParamTypes] = None,
                headers: Optional[HeaderTypes] = None,
                cookies: Optional[CookieTypes] = None,
                files: Optional[RequestFiles] = None,
                json: Any = None) -> Union[Response, Exception]:
        """
        A near-complete subset of httpx.request(...).
        Throws an `SgRequestError` in the case the result isn't in the 2XX range.
        """
        return self.__execute_http_sync(
            lambda: self._client().request(method=method,
                                           url=url,
                                           data=data,
                                           params=params,
                                           headers=headers,
                                           cookies=cookies,
                                           files=files,
                                           json=json,
                                           allow_redirects=True))

    def get(self,
            url: str,
            data: Optional[RequestData] = None,
            params: Optional[QueryParamTypes] = None,
            headers: Optional[HeaderTypes] = None,
            cookies: Optional[CookieTypes] = None,
            files: Optional[RequestFiles] = None,
            json: Any = None) -> Union[Response, Exception]:
        return self.request(url=url,
                            method='GET',
                            data=data,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            files=files,
                            json=json)

    def post(self,
             url: str,
             data: Optional[RequestData] = None,
             params: Optional[QueryParamTypes] = None,
             headers: Optional[HeaderTypes] = None,
             cookies: Optional[CookieTypes] = None,
             files: Optional[RequestFiles] = None,
             json: Any = None) -> Union[Response, Exception]:
        return self.request(url=url,
                            method='POST',
                            data=data,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            files=files,
                            json=json)

    def put(self,
            url: str,
            data: Optional[RequestData] = None,
            params: Optional[QueryParamTypes] = None,
            headers: Optional[HeaderTypes] = None,
            cookies: Optional[CookieTypes] = None,
            files: Optional[RequestFiles] = None,
            json: Any = None) -> Union[Response, Exception]:
        return self.request(url=url,
                            method='PUT',
                            data=data,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            files=files,
                            json=json)

    def head(self,
             url: str,
             data: Optional[RequestData] = None,
             params: Optional[QueryParamTypes] = None,
             headers: Optional[HeaderTypes] = None,
             cookies: Optional[CookieTypes] = None,
             files: Optional[RequestFiles] = None,
             json: Any = None) -> Union[Response, Exception]:
        return self.request(url=url,
                            method='HEAD',
                            data=data,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            files=files,
                            json=json)

    def delete(self,
               url: str,
               data: Optional[RequestData] = None,
               params: Optional[QueryParamTypes] = None,
               headers: Optional[HeaderTypes] = None,
               cookies: Optional[CookieTypes] = None,
               files: Optional[RequestFiles] = None,
               json: Any = None) -> Union[Response, Exception]:
        return self.request(url=url,
                            method='DELETE',
                            data=data,
                            params=params,
                            headers=headers,
                            cookies=cookies,
                            files=files,
                            json=json)

    def set_proxy_url(self, proxy_url: str):
        os.environ.update({'PROXY_URL': proxy_url})
        self.__refresh_client()

    def _mk_client(self, proxy_url: Optional[str]) -> BaseClient:
        return httpx.Client(proxies=proxy_url,
                            trust_env=False,
                            timeout=self.__timeout,
                            verify=self.__verify_ssl,
                            transport=httpx.HTTPTransport(retries=SgRequestsBase._CONNECTION_RETRIES,
                                                          verify=self.__verify_ssl,
                                                          trust_env=False,
                                                          http2=True)
                            )
