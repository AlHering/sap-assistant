# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from typing import Optional, Any
import scrapy
from scrapy import Spider
from scraping_service.model.website_archiver import WebsiteArchiver
from scraping_service.utility import scrapy_utility


class ArchiverSpider(Spider):
    """
    Archiver Spider class.
    """
    def __init__(self, archiver: WebsiteArchiver) -> None:
        """
        Initiation method for Archiver Spiders.
        :param archiver: Archiver instance.
        """
        self.archiver = archiver

    def parse(self, response: scrapy.http.Response) -> None:
        """
        Method for crawling page.
        """
        url = response.url
        self.archiver.register_page(response.meta.get("last_url"), url, response.body)

        target_pages = list(set([self.archiver.fix_link(url, elem) for elem in
                                 scrapy_utility.safely_get_elements(response, "//@href")]))
        target_assets = list(
            set([self.archiver.fix_link(url, elem) for elem in
                 scrapy_utility.safely_get_elements(response, "//@src | //@data-src")]))
        for page_link in [link for link in target_pages if
                          "." not in link.split("/")[-1] or ".html" in link.split("/")[-1].lower()]:
            if page_link not in target_assets:
                target_assets.append(page_link)
            target_pages.remove(page_link)

        for link in target_assets:
            if link not in self.archiver.crawled_assets:
                self.archiver.crawled_assets.append(link)
                self.archiver.register_asset(url, link, *self.archiver.get_asset_data(link))
        for link in target_pages:
            if link not in self.archiver.crawled_pages:
                self.archiver.crawled_pages.append(link)
                yield scrapy.Request(link,
                                     meta={
                                         "last_url": url
                                     },
                                     callback=self.parse)


class ScrapyWebsiteArchiver(WebsiteArchiver):
    """
    Website Archiver class based scrapy framework.
    """
    def __init__(self, profile: dict) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
        """
        super().__init__(profile)
        self.settings = profile.get("framework_arguments")
        self.args = [self]

    def archive_website(self) -> None:
        """
        Method for archiving website.
        """
        scrapy_utility.start_crawl_process(ArchiverSpider, self.args, self.settings)
