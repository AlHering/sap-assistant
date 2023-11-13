# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
import time
import requests
import copy
from typing import Union, Any, Optional, List
from urllib.parse import urlparse
from lxml import html
import traceback
from lxml.etree import ParseError
from requests.exceptions import SSLError
from urllib3.exceptions import MaxRetryError
from src.model.scraping_control.archiving.website_archiver import WebsiteArchiver
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility, time_utility, dictionary_utility, requests_utility
from src.utility.silver import internet_utility


class RequestsWebsiteArchiver(WebsiteArchiver):
    """
    Website Archiver class based requests framework.
    """

    def __init__(self, profile: dict, reload_last_state: bool = True) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
        :param reload_last_state: Flag declaring whether to reload last state from cache dumps.
        """
        super().__init__(profile=profile, reload_last_state=reload_last_state)
        self.cache["session"] = requests.Session()
        if isinstance(self.proxies, dict):
            self.cache["session"].proxies.update(self.proxies)
        elif isinstance(self.proxies, str) and self.proxies == "torsocks":
            self.cache["session"].proxies = {
                "http": "socks5://127.0.0.1:9050",
                "https": "socks5://127.0.0.1:9050"
            }
        self.cache["milestones"] = self.profile.get("milestones", 300)
        self.cache["last_milestone"] = self.cache.get("last_milestone", 0)
        self.cache["last_url"] = self.cache.get("last_url")
        self.cache["current_url"] = self.cache.get("current_url")
        self.cache["current_index"] = self.cache.get("current_index", 0)
        self.cache["reconnect_interval"] = self.profile.get(
            "reconnect_interval", 60)
        self.cache["reconnect_retries"] = self.profile.get(
            "reconnect_retries", 60)

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        try:
            next_url = self.get_next_url(self.cache["current_url"])
            while next_url is not None:
                if self.cache["current_index"] % self.cache["milestones"] == 0 and self.cache["current_index"] != 0:
                    self.cache["last_milestone"] = self.cache["current_index"]
                    self.create_state_dump(
                        reason="archiving_milestone")
                self._handle_next_page(next_url)
                self.cache["last_url"] = self.cache["current_url"]
                next_url = self.get_next_url(self.cache["current_url"])
        except Exception as ex:
            self.create_state_dump(
                reason={
                    "exception": str(ex),
                    "traceback": traceback.format_exc(),
                }
            )
            raise ex
        self.create_state_dump(reason="archiving_finished")

    def create_state_dump(self, reason: Optional[Any] = None) -> None:
        """
        Method for creating state dump of archiver.
        :param reason: Reason for state dump.
        :param final: Flag declaring, whether process is finished.
        """
        self.cache["failed"] = self.failed
        self.cache["reason"] = reason
        self.save_state(["session"], finished=isinstance(
            reason, str) and reason == "archiving_finished")

    def load_state_dump(self, path: str) -> None:
        """
        Method for loading state dump of archiver.
        :param path: Arbitrary arguments.
        """
        self.load_state()
        self.failed = set(self.cache.get("failed", []))

    def _retry_with_new_identity(self) -> requests.Response:
        """
        Internal method for retrying a requests with new identity.
        :param next_url: Target URL.
        :return: Response.
        """
        self.logger.info(
            f"[{self.profile['base_url']}] Status invalid, retrying with different user-agent and proxy setting '{self.proxies}' ...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
        response = self.cache["session"].get(
            self.cache["current_url"], headers=headers)
        self.logger.info(
            f"[{self.profile['base_url']}] User-agent change resulted in status '{response.status_code}' ...")
        if response is None or response.status_code != 200:
            if isinstance(self.proxies, str):
                if self.proxies == "random":
                    self.cache["session"].proxies = internet_utility.get_proxy(
                        source="package")
                    return self.cache["session"].get(self.cache["current_url"], headers=headers)

        return response if (response is not None and response.status_code != 200) else self.cache["session"].get(self.cache["current_url"], headers={"User-agent": internet_utility.get_user_agent()})

    def _request_page(self) -> Optional[requests.Response]:
        """
        Internal method for requesting page and handling errors.
        """
        response = None
        try:
            self.logger.info(
                f"[{self.profile['base_url']}] Fetching {self.cache['current_url']} ({self.cache['current_index']})")
            response = self.cache["session"].get(self.cache["current_url"])
            if response.status_code != 200:
                response = self._retry_with_new_identity()
        except MaxRetryError:
            self.logger.warning(
                f"[{self.profile['base_url']}] Max retry error appeared! Trying with different identity...")
            response = self._retry_with_new_identity()
        except SSLError:
            self.logger.warning(
                f"[{self.profile['base_url']}] SSL error appeared! Passing verification.")
            response = self.cache["session"].get(
                self.cache["current_url"], verify=False)
        except requests.exceptions.MissingSchema:
            self.logger.warning(
                f"[{self.profile['base_url']}] Missing schema! Trying to fix URL.")
            self.cache["current_url"] = self.fix_link(
                self.cache["last_url"], self.cache["current_url"])
            response = self.cache["session"].get(self.cache["current_url"])
        except requests.exceptions.TooManyRedirects:
            self.failed.add(self.cache["current_url"])
            self.logger.info(
                f"[{self.profile['base_url']}] '{self.cache['current_url']}' exceeded limit of redirects, ignoring ...")
        except requests .exceptions.ConnectionError as ex:
            self.create_state_dump(
                reason={
                    "exception": str(ex),
                    "traceback": traceback.format_exc()
                }
            )
            # TODO: Implement appropriate methods on super-class level
            self.logger.warning(
                f"[{self.profile['base_url']}] Connection error appeared! State dump created.")
            if not internet_utility.check_connection():
                tries = 0
                while not internet_utility.check_connection() and self.cache["reconnect_retries"] < tries:
                    self.logger.warning(
                        f"[{self.profile['base_url']}] No internet connection! Retrying in 10 seconds ...")
                    time.sleep(self.cache["reconnect_interval"])
                    tries += 1
                self.logger.info(
                    f"[{self.profile['base_url']}] Regained internet connection, retrying ...")
                return self._handle_next_page(self.cache["current_url"])
            else:
                self.failed.add(self.cache["current_url"])
                self.logger.info(
                    f"[{self.profile['base_url']}] '{self.cache['current_url']}' not reachable, ignoring ...")
        return response

    def _handle_next_page(self, next_url: str) -> None:
        """
        Internal method to handle next page.
        :param next_url: Next page URL.
        """
        self.cache["current_url"] = next_url
        response = self._request_page()
        if response is not None and response.status_code == 200:
            # Processing response
            self.logger.info(
                f"[{self.profile['base_url']}] Status: {response.status_code}")
            self.register_page(self.cache["current_url"], response.content)

            html_content = html.fromstring(
                response.content if response.content else "<!DOCTYPE html><html>")

            target_pages = list(
                set([self.fix_link(response.url, elem) for elem in html_content.xpath("//@href | //@src | //@data-src")]))
            target_assets = []
            for page_link in [link for link in target_pages if
                              "." in urlparse(link).path.split("/")[-1] and ".html" not in urlparse(link).path.split("/")[
                                  -1].lower() and ".php" not in urlparse(link).path.split("/")[
                                  -1].lower()]:
                if page_link not in target_assets:
                    target_assets.append(page_link)
                target_pages.remove(page_link)

            for link in target_assets:
                self.register_link(self.cache["current_url"], link, "asset")
                try:
                    asset_data = self.get_asset_data(link)
                    self.register_asset(
                        self.cache["current_url"], link, *asset_data)
                except requests.exceptions.MissingSchema:
                    self.logger.info(
                        f"[{self.profile['base_url']}] Schema exception appeared for '{link}'")
                except requests.exceptions.ConnectionError:
                    self.logger.info(
                        f"[{self.profile['base_url']}] ConnectionError exception appeared for '{link}'")

            discarded = 0
            discarded_external = 0
            for link in target_pages:
                link_netloc = urlparse(link).netloc
                if any(base in link_netloc for base in self.allowed_bases):
                    newly_created = self.register_link(
                        self.cache["current_url"], link, "page")
                    if not newly_created:
                        discarded += 1
                else:
                    discarded_external += 1
            self.logger.info(
                f"[{self.profile['base_url']}] Discarded {discarded} internal and {discarded_external} external page links.")
            self.cache["current_index"] += 1
