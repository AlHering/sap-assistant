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

    def __init__(self, profile: dict) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
        """
        super().__init__(profile)
        self._cache["session"] = requests.Session()
        self._cache["structure"] = {}
        self.crawled_pages = [self.base_url]
        self._cache["current_index"] = 0
        self._cache["milestones"] = self.profile.get("milestones", 1000)
        self.next_proxy = self.profile.get("proxies", "random")

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        try:
            while self._cache["current_index"] < len(self.crawled_pages):
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
                               "METADATA_collection_finished.json")
        self.relink_temporary_links()
        self.create_state_dump("relinking_finished", "METADATA_final.json")

    def create_state_dump(self, reason: Optional[Any] = None, file_name: str = "EXCEPTION.json") -> None:
        """
        Method for creating state dump of archiver.
        :param reason: Reason for state dump.
        :param file_name: File name. Defaults to "EXCEPTION.json".
        """
        json_utility.save(
            {
                "_cache": {key: self._cache[key] for key in self._cache if key != "session"},
                "crawled_pages": self.crawled_pages,
                "crawled_assets": self.crawled_assets,
                "reason": reason
            },
            os.path.join(
                self.profile.get("offline_copy_path", cfg.PATHS.DUMP_PATH),
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
        self.crawled_pages = dump_data["crawled_pages"]
        self.crawled_assets = dump_data["crawled_assets"]

    def _handle_next_page(self) -> None:
        """
        Internal method to handle next page.
        """
        current_link = self.crawled_pages[0]
        try:
            self.logger.info(
                f"Fetching {current_link} at index {self._cache['current_index']} with {len(self.crawled_pages)} waiting ...")
            response = self._cache["session"].get(current_link)
        except SSLError:
            self.logger.warning(f"SSL error appeared! Passing verification.")
            response = self._cache["session"].get(current_link, verify=False)
        except requests.exceptions.MissingSchema:
            current_link = f"https:{current_link}"
            self.crawled_pages[0] = current_link
            self.logger.info(
                f"Fetching {current_link} at index {self._cache['current_index']} with {len(self.crawled_pages)} waiting ...")
            response = self._cache["session"].get(current_link)
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
        self.register_page(current_link, response.content)

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
            if not dictionary_utility.exists(self._cache["structure"], link.split("/")):
                try:
                    asset_data = self.get_asset_data(link)
                    self.register_asset(current_link, link, *asset_data)
                    dictionary_utility.set_and_extend_nested_field(
                        self._cache["structure"], link.split("/"), {"#meta_type": "asset"})
                except requests.exceptions.MissingSchema:
                    self.logger.info(f"Schema exception appeared for '{link}'")
            else:
                self.register_link(current_link, link, "asset")

        self.register_temporary_page_links(current_link, target_pages)
        discarded = 0
        discarded_external = 0
        for link in target_pages:
            link_netloc = urlparse(link).netloc
            if any(base in link_netloc for base in self.allowed_bases):
                if not dictionary_utility.exists(self._cache["structure"], link.split("/") + ["#meta_type"]):
                    dictionary_utility.set_and_extend_nested_field(
                        self._cache["structure"], link.split("/"), {"#meta_type": "page"})
                    self.crawled_pages.append(link)
                else:
                    discarded += 1
            else:
                discarded_external += 1
        self.logger.info(
            f"Discarded {discarded} internal and {discarded_external} external page links.")
        self.crawled_pages = self.crawled_pages[1:]
        self._cache["current_index"] += 1
