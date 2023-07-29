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
        self._cache["milestones"] = self.profile.get("milestones", 1000)
        self._cache["current_url"] = self._cache.get("current_url")
        self._cache["current_index"] = self._cache.get("current_index", 0)
        self.next_proxy = self.profile.get("proxies", "random")

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        try:
            while self.get_next_url(self._cache["current_url"]) is not None:
                if self._cache["current_index"] % self._cache["milestones"] == 0:
                    self.create_state_dump(
                        "crawling_milestone", f"MILESTONE_{self._cache['current_index']}.json")
                self._handle_next_page()
        except Exception as ex:
            self.create_state_dump({
                "exception": str(ex),
                "traceback": traceback.format_exc()
            })
            raise ex
        self.create_state_dump("collection_finished",
                               f"MILESTONE_{self._cache['current_index']}_FINISHED.json")

    def create_state_dump(self, reason: Optional[Any] = None, file_name: str = "EXCEPTION.json") -> None:
        """
        Method for creating state dump of archiver.
        :param reason: Reason for state dump.
        :param file_name: File name. Defaults to "EXCEPTION.json".
        """
        json_utility.save(
            {
                "_cache": {key: self._cache[key] for key in self._cache if key != "session"},
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

    def _handle_next_page(self) -> None:
        """
        Internal method to handle next page.
        """
        self._cache["current_url"] = self.get_next_url(
            self._cache["current_url"])
        try:
            self.logger.info(
                f"Fetching {self._cache['current_url']}")
            response = self._cache["session"].get(self._cache["current_url"])
        except SSLError:
            self.logger.warning(f"SSL error appeared! Passing verification.")
            response = self._cache["session"].get(
                self._cache["current_url"], verify=False)
        except requests.exceptions.ConnectionError as ex:
            self.create_state_dump({
                "exception": str(ex),
                "traceback": traceback.format_exc()
            })
            # TODO: Implement appropriate methods on super-class level
            self.logger.warning(f"Connection error appeared! Dump created!")
            while not internet_utility.check_connection():
                self.logger.warning(
                    f"No internet connection! Retrying in 10 seconds ...")
                time.sleep(10)
            self.logger.info(f"Using proxy: '{self.next_proxy}'")
            self._cache["session"] = requests_utility.get_session(
                proxy_flag=self.next_proxy)
            if self.next_proxy in ["torsocks", "random"]:
                self.next_proxy = {"torsocks": "random",
                                   "random": "torsocks"}[self.next_proxy]
            return
        self.logger.info(f"Status: {response.status_code}")
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
                dictionary_utility.set_and_extend_nested_field(
                    self._cache["structure"], link.split("/"), {"#meta_type": "asset"})
            except requests.exceptions.MissingSchema:
                self.logger.info(f"Schema exception appeared for '{link}'")

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
            f"Discarded {discarded} internal and {discarded_external} external page links.")
        self._cache["current_index"] += 1
