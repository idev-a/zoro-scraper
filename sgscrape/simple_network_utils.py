"""
Simple network utilities


## TODO
create a 'get_headers' method to use a headless-browser to make a call,
and return all the request headers.

"""
from requests import Response, RequestException
from urllib3 import Retry

from .simple_utils import *
from sgrequests import SgRequests
import bs4
import urllib.parse
from bs4 import BeautifulSoup
from requests_toolbelt.utils import dump
import sglogging

log = sglogging.SgLogSetup().get_logger('simple_network_utils')

def fetch_json(request_url: str,
               method: str = 'GET',
               path_to_locations: List[str] = [],
               query_params: dict = {},
               data_params: dict = {},
               headers: dict = {},
               retries: Optional[Retry] = SgRequests.DEFAULT_RETRY_BEHAVIOR,
               proxy_rotation_failure_threshold=10) -> List[dict]:
    """
    Fetches locations json, and returns as a list of parsed dictionaries.
    Assumes a regular structure with root element, and sibling-elements.

    :param path_to_locations: the path, in the json, to the locations array.
    :param method: See `fetch_data`
    :param request_url: See `fetch_data`
    :param query_params: See `fetch_data`
    :param data_params: See `fetch_data`
    :param headers: See `fetch_data`
    :param retries: See `fetch_data`
    :param retry_backoff_factor: See `fetch_data`

    :return: a list of parsed dictionaries
    """

    response = fetch_data(request_url=request_url,
                          method=method,
                          data_params=data_params,
                          query_params=query_params,
                          headers=headers,
                          retries=retries,
                          proxy_rotation_failure_threshold=proxy_rotation_failure_threshold)
    json_result = response.json()

    return [drill_down_into(json_result, path_to_locations)]

def fetch_xml(request_url: str,
              root_node_name: str,
              location_node_name: str,
              method: str = 'GET',
              location_parser: Callable[[bs4.Tag], dict] = xml_to_dict,
              location_node_properties: Dict[str, str] = {},
              query_params: dict = {},
              data_params: dict = {},
              headers: dict = {},
              xml_parser: str = 'lxml',
              retries: Optional[Retry] = SgRequests.DEFAULT_RETRY_BEHAVIOR,
              proxy_rotation_failure_threshold=10) -> List[dict]:
    """
    Fetches locations xml, and returns as a list of parsed dictionaries.
    Assumes a regular structure with root element, and sibling-elements.

    :param root_node_name: root element under which locations are stored.
    :param location_node_name: element name of each location.
    :param location_node_properties: the properties of the node, e.g. {'class':'abc def'}
    :param location_parser: function that converts a location Tag into a dict.
    :param request_url: See `fetch_data`
    :param method: See `fetch_data`
    :param query_params: See `fetch_data`
    :param data_params: See `fetch_data`
    :param headers: See `fetch_data`
    :param xml_parser: The xml parser used by BeautifulSoup
    :param retries: See `fetch_data`
    :param retry_backoff_factor: See `fetch_data`

    :return: a generator of parsed dictionaries
    """

    response = fetch_data(request_url=request_url,
                          method=method,
                          data_params=data_params,
                          query_params=query_params,
                          headers=headers,
                          retries=retries,
                          proxy_rotation_failure_threshold=proxy_rotation_failure_threshold)

    xml_result = BeautifulSoup(response.text, xml_parser)

    root_node = xml_result.find(root_node_name)

    location_nodes = root_node.find_all(location_node_name, location_node_properties)

    for location in location_nodes:
        yield location_parser(location)

def fetch_html(request_url: str,
               method: str = 'GET',
               location_parser: Callable[[bs4.Tag], dict] = xml_to_dict,
               location_node_properties: Dict[str, str] = {},
               query_params: dict = {},
               data_params: dict = {},
               headers: dict = {},
               xml_parser: str = 'lxml',
               retries: Optional[Retry] = SgRequests.DEFAULT_RETRY_BEHAVIOR,
               proxy_rotation_failure_threshold=10) -> List[dict]:
    """
    Just like `fetch_xml`, but with root_node_name=html, location_node_name=body
    """
    return fetch_xml(request_url=request_url,
                     method=method,
                     root_node_name='html',
                     location_node_name='body',
                     location_node_properties=location_node_properties,
                     location_parser=location_parser,
                     query_params=query_params,
                     data_params=data_params,
                     headers=headers,
                     xml_parser=xml_parser,
                     retries=retries,
                     proxy_rotation_failure_threshold=proxy_rotation_failure_threshold)


def fetch_data(request_url: str,
               method: str = 'GET',
               allow_redirects = True,
               query_params: dict = {},
               data_params: dict = {},
               headers: dict = {},
               retries: Optional[Retry] = SgRequests.DEFAULT_RETRY_BEHAVIOR,
               proxy_rotation_failure_threshold=10) -> Response:
    """
    Returns the `SgRequests` response if the response status code is in (200, 299)

    :param request_url: The URL to fetch.
    :param method: Uppercase standard HTTP method.
    :param allow_redirects: Defaults to True.
    :param query_params: The raw query params, unquoted and unescaped.
    :param data_params: The raw form data.
    :param headers: The headers.
    :param retries: How many retries to attempt.
    :param proxy_rotation_failure_threshold: How many failures to swallow before rotating proxies?
    :return:
    """
    with SgRequests(retry_behavior=retries,
                    proxy_rotation_failure_threshold=proxy_rotation_failure_threshold) as session:
        response = session.request(method=method,
                                   url=request_url,
                                   data=data_params,
                                   params=query_params,
                                   headers=headers,
                                   allow_redirects=allow_redirects)

        if response.status_code < 200 or response.status_code > 299:
            log.error("API call is not successful; result status: " + str(response.status_code))
            log.error(dump.dump_all(response).decode("utf-8"))
            raise RequestException(f'Bad status code: {response.status_code}', response=response)

        return response


def paginated(fetch_results: Callable[[int], list], max_per_page: int, first_page: int = 1) -> list:
    """
    Simple utility to fetch paginated results.

    :param fetch_results: Given the page number, fetch the list (or Generator) of results from that page.
    :param max_per_page: The expected cap of results per page; if the count falls below it, the function will terminate.
    :param first_page: What's the first page? Defaults to 1. Page numbers will be incremented from this one.
    :return: The generator of results from all pages.
    """
    keep_going = True
    page_num = first_page
    while keep_going:
        result_count = 0
        for result in fetch_results(page_num):
            yield result
            result_count += 1

        if result_count < max_per_page:
            keep_going = False
        else:
            page_num += 1
