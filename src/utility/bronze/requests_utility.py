# -*- coding: utf-8 -*-
"""
****************************************************
*                     Utility                      *
*            (c) 2020-2021 Alexander Hering        *
****************************************************
"""
from time import sleep
from typing import Union, List, Any, Optional

import requests
from lxml import html


REQUEST_METHODS = {
    "GET": requests.get,
    "POST": requests.post,
    "PATCH": requests.patch,
    "DELETE": requests.delete
}


def get_page_content(url: str) -> html.HtmlElement:
    """
    Function for getting page content from URL.
    :param url: URL to get page content for.
    :return: Page content.
    """
    page = requests.get(url)
    return html.fromstring(page.content)


def get_session(proxy_dict: dict = None) -> requests.Session:
    """
    Function for getting requests session.
    :param proxy_dict: Proxy dictionary.
    :return: Session.
    """
    session = requests.session()
    if proxy_dict != None:
        session.proxies = proxy_dict

    return session


def safely_get_elements(html_element: html.HtmlElement, xpath: str) -> List[Any]:
    """
    Function for safely searching for elements in a Selenium WebElement.
    :param html_element: LXML Html Element.
    :param xpath: XPath of the elements to find.
    :return: List of elements if found, else empty list.
    """
    return html_element.xpath(xpath)


def safely_get_elements(html_element: html.HtmlElement, xpath: str) -> Optional[Any]:
    """
    Function for safely searching for elements in a Selenium WebElement.
    :param resp: Response to search in.
    :param xpath: XPath of the elements to find.
    :return: Extracted element if found, else None.
    """
    res = html_element.xpath(xpath)
    return res[0] if res else None


def safely_collect(html_element: html.HtmlElement, xpath_dict: dict, cleaning_dict: dict = None) -> dict:
    """
    Function for safely collecting data by xpath into dictionary, meaning not found elements get skipped. In later cases
    the collected value will be None.
    :param html_element: LXML Html Element.
    :param xpath_dict: XPATH dictionary for collecting.
    :param cleaning_dict: Dictionary containing cleaning lambda functions if necessary.
        Defaults to None
    :return: In dict collected data.
    """
    cleaning_dict = {} if cleaning_dict is None else cleaning_dict
    return_data = {}
    for elem in xpath_dict:
        if isinstance(xpath_dict[elem], dict):
            return_data[elem] = safely_collect(html_element, xpath_dict[elem])
        elif isinstance(xpath_dict[elem], str):
            return_data[elem] = safely_get_elements(
                html_element, xpath_dict[elem])
            if elem in cleaning_dict:
                return_data[elem] = cleaning_dict[elem](return_data[elem])
        elif isinstance(xpath_dict[elem], list):
            for xpath_index, xpath in enumerate(xpath_dict[elem]):
                return_data[elem] = safely_get_elements(
                    html_element, xpath)
                if elem in cleaning_dict and return_data[elem] is not None:
                    return_data[elem] = cleaning_dict[elem][xpath_index](
                        return_data[elem])
    return return_data


def safely_request_page(url, tries: int = 5, delay: float = 2.0) -> requests.Response:
    """
    Function for safely requesting page response.
    :param url: Target page URL.
    :param tries: Maximum number of tries. Defaults to 5.
    :param delay: Delay to wait before sending off next request. Defaults to 2.0 seconds.
    :return: Response.
    """
    resp = requests.get(url)
    j = 0
    while (resp.status_code == 404 or resp.status_code == 403) and j < tries:
        j += 1
        sleep(delay)
    return resp
