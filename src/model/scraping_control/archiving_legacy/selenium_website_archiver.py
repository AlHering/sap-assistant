# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from src.model.scraping_control.archiving_legacy.website_archiver import WebsiteArchiver
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility, time_utility, selenium_utility
from src.utility.silver import internet_utility


class SeleniumWebsiteArchiver(WebsiteArchiver):
    """
    Website Archiver class based selenium framework.
    """

    def __init__(self, profile: dict) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
        """
        super().__init__(profile)
        self._cache["driver"] = selenium_utility.get_driver(
            **profile.get("framework_arguments", {}))
        self._cache["last_link"] = None
        self._cache["current_link"] = self.base_url

    def archive_website(self) -> None:
        """
        Method for archiving website.
        :param args: Arbitrary arguments.
        :param kwargs: Arbitrary keyword arguments.
        """
        if any(base in self._cache["current_link"] for base in self.allowed_bases):
            response = self._cache["driver"].get(self._cache["current_link"])
            self.register_page(
                self._cache["last_link"], self._cache["current_link"], response.page_source)

            target_pages = list(set([self.fix_link(self._cache["driver"].current_url, elem) for elem in
                                     selenium_utility.safely_get_elements(self._cache["driver"], "//@href")]))
            target_assets = list(
                set([self.fix_link(self._cache["driver"].current_url, elem) for elem in
                     selenium_utility.safely_get_elements(self._cache["driver"], "//@src | //@data-src")]))
            for page_link in [link for link in target_pages if
                              "." not in link.split("/")[-1] or ".html" in link.split("/")[-1].lower()]:
                if page_link not in target_assets:
                    target_assets.append(page_link)
                target_pages.remove(page_link)

            for link in target_assets:
                if link not in self.crawled_assets:
                    self.crawled_assets.append(link)
                    self.register_asset(
                        self._cache["current_link"], link, *self.get_asset_data(link))
            for link in target_pages:
                if link not in self.crawled_pages:
                    self.crawled_pages.append(link)
                    self._cache["last_link"] = self._cache["current_link"]
                    self._cache["current_link"] = link
                    self.archive_website()
