# -*- coding: utf-8 -*-
"""
****************************************************
*                     Utility                      *
*            (c) 2020-2021 Alexander Hering        *
****************************************************
"""
import os
import functools
import functools
import multiprocessing
from multiprocessing.pool import ThreadPool
import urllib3
import random
import socket
import logging
from time import sleep
from typing import Any, Optional

import requests
from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
LOGGER = logging.Logger("[InternetUtility]")


def check_connection() -> bool:
    """
    Function for checking internet connection, taken from
    @https://stackoverflow.com/questions/28752174/checking-internet-connection-with-python
    and adapted.
    :return: True, if internet connection is working, else False.
    """
    LOGGER.info("Checking internet connection...")
    try:
        requests.get("https://www.startpage.com/", timeout=10)
        LOGGER.info("Connection successfully established!")
        return True
    except requests.ConnectionError:
        LOGGER.warning("Connection not successfully established!")
        return False


def wait_for_connection() -> None:
    """
    Function for waiting for stable internet connection.
    """
    LOGGER.info("Waiting for internet connection...")
    while not check_connection():
        sleep(10.0)


def timeout(max_timeout: float) -> Any:
    """
    Timeout decorator, parameter in seconds.
    Taken from https://stackoverflow.com/questions/492519/timeout-on-a-function-call.
    """
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args: Optional[Any], **kwargs: Optional[Any]):
            """Closure for function."""
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            # raises a TimeoutError if execution exceeds max_timeout
            try:
                return async_result.get(max_timeout)
            except multiprocessing.context.TimeoutError:
                return "TIMOUT_DECORATOR_TIMEOUT_SIGNAL"
        return func_wrapper
    return timeout_decorator


def get_proxy(**kwargs: Optional[Any]) -> str:
    """
    Function for getting proxy.
    :param kwargs: :param kwargs: Arbitrary keyword arguments.
        'source': Source to get proxy from: 'package' or the path to a file, containing proxies, are supported.
        'location': Location for proxy to get, only working with supported source.
    :return: Proxy IP as string.
    """
    source = kwargs.get("source", "package")
    location = kwargs.get("location", "")

    if source == "package":
        proxies = RequestProxy().get_proxy_list()
        if location:
            for proxy in proxies:
                if proxy.country.lower() == location.lower():
                    return proxy.get_address()
        else:
            return proxies[random.randint(0, len(proxies)-1)].get_address()
    elif os.path.exists(source):
        proxies = open(source, "r").readlines()
        return proxies[random.randint(0, len(proxies)-1)]


def check_port_usable(port: int) -> bool:
    """
    Check, whether port is not in use. Taken from
    @https://stackoverflow.com/a/52872579 and adjusted.
    :param port: Port to check.
    :return: True, if port is not in use, else False.
    """
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect(("localhost", port))
        test_socket.close()
        sleep(2)
        return True
    except OSError:
        return False


def timeout(max_timeout: float) -> Any:
    """
    Timeout decorator, parameter in seconds.
    Taken from https://stackoverflow.com/questions/492519/timeout-on-a-function-call.
    :return: Wrapped function result, if process was successful, else
        'TIMOUT_DECORATOR_TIMEOUT_SIGNAL': Process timed out
        'URLLIB_PROTOCOL_ERROR_SIGNAL': Process resulted in urllib error.
    """
    def timeout_decorator(item):
        """Wrap the original function."""
        @functools.wraps(item)
        def func_wrapper(*args: Optional[Any], **kwargs: Optional[Any]):
            """Closure for function."""
            pool = ThreadPool(processes=1)
            async_result = pool.apply_async(item, args, kwargs)
            # raises a TimeoutError if execution exceeds max_timeout
            try:
                return async_result.get(max_timeout)
            except multiprocessing.context.TimeoutError:
                return "TIMOUT_DECORATOR_TIMEOUT_SIGNAL"
            except urllib3.exceptions.ProtocolError:
                return "URLLIB_PROTOCOL_ERROR_SIGNAL"
        return func_wrapper
    return timeout_decorator
