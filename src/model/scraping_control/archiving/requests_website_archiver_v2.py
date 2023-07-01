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
from typing import Union, Any, Optional
from urllib.parse import urlparse
from lxml import html
import traceback
from lxml.etree import ParseError
from requests.exceptions import SSLError
from scraping_service.model.website_archiver import WebsiteArchiver
from scraping_service.configuration import configuration as cfg
from scraping_service.utility import internet_utility, json_utility, time_utility, requests_utility


class RequestsWebsiteArchiver(WebsiteArchiver):
    """
    Website Archiver class based requests framework.
    """

    def __init__(self, profile: dict) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
        """
        super().__init__(profile)
        self._cache["session"] = requests.Session()
        self.crawled_pages = [self.base_url]
        self._cache["children"] = {}
        self._cache["current_index"] = 0
        self._cache["last_dump"] = None
        self.next_proxy = self.profile.get("proxies", "random")

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        try:
            while self._cache["current_index"] < len(self.crawled_pages):
                if self._cache["current_index"] % 100 == 0:
                    if self._cache["last_dump"] is not None and os.path.exists(self._cache["last_dump"]):
                        os.remove(self._cache["last_dump"])
                    self._cache["last_dump"] = self.create_state_dump("milestone")
                self._handle_next_page()
        except Exception as ex:
            self.create_state_dump({
                "exception": str(ex),
                "traceback": traceback.format_exc()
            })
            return
        if self._cache["last_dump"] is not None and os.path.exists(self._cache["last_dump"]):
            os.remove(self._cache["last_dump"])
        self.create_state_dump("finished")

    def create_state_dump(self, reason: Optional[Any] = None) -> str:
        """
        Method for creating state dump of archiver.
        :param reason: Reason for state dump.
        return: File path.
        """
        path = os.path.join(
                self.profile.get("offline_copy_path", cfg.PATHS.DUMP_PATH),
                f"EXCEPTION_{time_utility.get_timestamp()}.json"
            )
        json_utility.save(
            {
                "_cache": {key: self._cache[key] for key in self._cache if key != "session"},
                "crawled_pages": self.crawled_pages,
                "crawled_assets": self.crawled_assets,
                "reason": reason
            },
            path
        )
        return path

    def load_state_dump(self, path: str) -> None:
        """
        Method for loading state dump of archiver.
        :param path: Arbitrary arguments.
        """
        dump_data = json_utility.load(path)
        self._cache.update(dump_data["_cache"])
        self.crawled_pages = dump_data["crawled_pages"]
        self.crawled_assets = dump_data["crawled_assets"]

    def _handle_next_page(self) -> None:
        """
        Internal method to handle next page.
        """
        last_link = self._cache["children"].get(self.crawled_pages[self._cache["current_index"]], None)
        current_link = self.crawled_pages[self._cache["current_index"]]
        try:
            self.logger.info(f"Fetching {current_link} under source {last_link}")
            response = self._cache["session"].get(current_link)
        except SSLError:
            self.logger.warning(f"SSL error appeared! Passing verification.")
            response = self._cache["session"].get(current_link, verify=False)
        except requests.exceptions.ConnectionError as ex:
            self.create_state_dump({
                "exception": str(ex),
                "traceback": traceback.format_exc()
            })
            # TODO: Implement appropriate methods on super-class level
            self.logger.warning(f"Connection error appeared! Dump created!")
            while not internet_utility.check_connection():
                self.logger.warning(f"No internet connection! Retrying in 10 seconds ...")
                time.sleep(10)
            self.logger.info(f"Using proxy: '{self.next_proxy}'")
            self._cache["session"] = requests_utility.get_session(proxy_flag=self.next_proxy)
            if isinstance(self.next_proxy, str):
                self.next_proxy = {"torsocks": "random", "random": "torsocks"}[self.next_proxy]
            return
        self.logger.info(f"Status: {response.status_code}")
        self.register_page(last_link, current_link, response.content)

        if last_link:
            self.register_link(last_link, current_link, "page")

        html_content = html.fromstring(response.content if response.content else "<!DOCTYPE html><html>")

        target_pages = list(
            set([self.fix_link(response.url, elem) for elem in html_content.xpath("//@href | //@src | //@data-src")]))
        target_assets = []
        for page_link in [link for link in target_pages if
                          "." in urlparse(link).path.split("/")[-1] and ".html" not in urlparse(link).path.split("/")[
                              -1].lower()]:
            if page_link not in target_assets:
                target_assets.append(page_link)
            target_pages.remove(page_link)

        for link in target_assets:
            if link not in self.crawled_assets:
                self.crawled_assets.append(link)
                self.register_asset(current_link, link, *self.get_asset_data(link))
            else:
                self.register_link(current_link, link, "asset")
        for link in target_pages:
            link_netloc = urlparse(link).netloc
            if link not in self.crawled_pages and any(base in link_netloc for base in self.allowed_bases):
                self.crawled_pages.append(link)
                self._cache["children"][link] = current_link
        self._cache["current_index"] += 1
