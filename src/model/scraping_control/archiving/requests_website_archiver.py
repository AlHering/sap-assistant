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
        self._cache["session"] = requests.Session()
        if isinstance(self.proxies, dict):
            self._cache["session"].proxies.update(self.proxies)
        elif isinstance(self.proxies, str) and self.proxies == "torsocks":
            self._cache["session"].proxies = {
                "http": "socks5://127.0.0.1:9050",
                "https": "socks5://127.0.0.1:9050"
            }
        self._cache["milestones"] = self.profile.get("milestones", 300)
        self._cache["last_milestone"] = self._cache.get("last_milestone", 0)
        self._cache["last_url"] = self._cache.get("last_url")
        self._cache["current_url"] = self._cache.get("current_url")
        self._cache["current_index"] = self._cache.get("current_index", 0)
        self._cache["reconnect_interval"] = self.profile.get(
            "reconnect_interval", 60)
        self._cache["reconnect_retries"] = self.profile.get(
            "reconnect_retries", 3600)

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        try:
            next_url = self.get_next_url(self._cache["current_url"])
            while next_url is not None:
                if self._cache["current_index"] % self._cache["milestones"] == 0 and self._cache["current_index"] != 0:
                    latest_path = os.path.join(self.dump_folder, "latest.json")
                    if os.path.exists(latest_path):
                        os.rename(latest_path, os.path.join(
                            self.dump_folder, f"MILESTONE_{self._cache['last_milestone']}.json"))
                    self._cache["last_milestone"] = self._cache["current_index"]
                    self.create_state_dump(
                        reason="archiving_milestone")
                self._handle_next_page(next_url)
                self._cache["last_url"] = self._cache["current_url"]
                next_url = self.get_next_url(self._cache["current_url"])
        except Exception as ex:
            self.create_state_dump(
                reason={
                    "exception": str(ex),
                    "traceback": traceback.format_exc(),
                },
                file_name="EXCEPTION.json"
            )
            raise ex
        self.create_state_dump(reason="archiving_finished",
                               file_name=f"MILESTONE_{self._cache['current_index']}_FINISHED.json")

    def create_state_dump(self, reason: Optional[Any] = None, file_name: str = "latest.json") -> None:
        """
        Method for creating state dump of archiver.
        :param reason: Reason for state dump.
        :param file_name: File name. Defaults to "latest.json".
        """
        json_utility.save(
            {
                "_cache": {key: self._cache[key] for key in self._cache if key != "session"},
                "failed": list(self.failed),
                "reason": reason
            },
            os.path.join(
                self.dump_folder,
                file_name
            )
        )

    def load_state_dump(self, path: str) -> None:
        """
        Method for loading state dump of archiver.
        :param path: Arbitrary arguments.
        """
        dump_data = json_utility.load(path)
        self._cache.update(dump_data["_cache"])
        self.failed = set(dump_data.get("failed", []))

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
        response = self._cache["session"].get(
            self._cache["current_url"], headers=headers)
        self.logger.info(
            f"[{self.profile['base_url']}] User-agent change resulted in status '{response.status_code}' ...")
        if response is None or response.status_code != 200:
            if isinstance(self.proxies, str):
                if self.proxies == "random":
                    self._cache["session"].proxies = internet_utility.get_proxy(
                        source="package")
                    return self._cache["session"].get(self._cache["current_url"], headers=headers)

        return response if (response is not None and response.status_code != 200) else self._cache["session"].get(self._cache["current_url"], headers={"User-agent": internet_utility.get_user_agent()})

    def _request_page(self) -> Optional[requests.Response]:
        """
        Internal method for requesting page and handling errors.
        """
        response = None
        try:
            self.logger.info(
                f"[{self.profile['base_url']}] Fetching {self._cache['current_url']} ({self._cache['current_index']})")
            response = self._cache["session"].get(self._cache["current_url"])
            if response.status_code != 200:
                response = self._retry_with_new_identity()
        except MaxRetryError:
            self.logger.warning(
                f"[{self.profile['base_url']}] Max retry error appeared! Trying with different identity...")
            response = self._retry_with_new_identity()
        except SSLError:
            self.logger.warning(
                f"[{self.profile['base_url']}] SSL error appeared! Passing verification.")
            response = self._cache["session"].get(
                self._cache["current_url"], verify=False)
        except requests.exceptions.MissingSchema:
            self.logger.warning(
                f"[{self.profile['base_url']}] Missing schema! Trying to fix URL.")
            self._cache["current_url"] = self.fix_link(
                self._cache["last_url"], self._cache["current_url"])
            response = self._cache["session"].get(self._cache["current_url"])
        except requests.exceptions.TooManyRedirects:
            self.failed.add(self._cache["current_url"])
            self.logger.info(
                f"[{self.profile['base_url']}] '{self._cache['current_url']}' exceeded limit of redirects, ignoring ...")
        except requests .exceptions.ConnectionError as ex:
            self.create_state_dump(
                reason={
                    "exception": str(ex),
                    "traceback": traceback.format_exc()
                },
                file_name="EXCEPTION.json"
            )
            # TODO: Implement appropriate methods on super-class level
            self.logger.warning(
                f"[{self.profile['base_url']}] Connection error appeared! State dump created.")
            if not internet_utility.check_connection():
                tries = 0
                while not internet_utility.check_connection() and self._cache["reconnect_retries"] < tries:
                    self.logger.warning(
                        f"[{self.profile['base_url']}] No internet connection! Retrying in 10 seconds ...")
                    time.sleep(self._cache["reconnect_interval"])
                    tries += 1
                self.logger.info(
                    f"[{self.profile['base_url']}] Regained internet connection, retrying ...")
                return self._handle_next_page(self._cache["current_url"])
            else:
                self.failed.add(self._cache["current_url"])
                self.logger.info(
                    f"[{self.profile['base_url']}] '{self._cache['current_url']}' not reachable, ignoring ...")
        return response

    def _handle_next_page(self, next_url: str) -> None:
        """
        Internal method to handle next page.
        :param next_url: Next page URL.
        """
        self._cache["current_url"] = next_url
        response = self._request_page()
        if response is not None and response.status_code == 200:
            # Processing response
            self.logger.info(
                f"[{self.profile['base_url']}] Status: {response.status_code}")
            self.register_page(self._cache["current_url"], response.content)

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
                self.register_link(self._cache["current_url"], link, "asset")
                try:
                    asset_data = self.get_asset_data(link)
                    self.register_asset(
                        self._cache["current_url"], link, *asset_data)
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
                        self._cache["current_url"], link, "page")
                    if not newly_created:
                        discarded += 1
                else:
                    discarded_external += 1
            self.logger.info(
                f"[{self.profile['base_url']}] Discarded {discarded} internal and {discarded_external} external page links.")
            self._cache["current_index"] += 1
