# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import requests
from urllib.parse import urlparse
from lxml import html
from abc import ABC, abstractmethod
from sqlalchemy import Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, func, select
from sqlalchemy.ext.automap import automap_base
from typing import Any, List, Tuple, Optional
import datetime
from src.configuration import configuration as cfg
from src.utility.bronze import sqlalchemy_utility, requests_utility
from src.utility.bronze.string_utility import clean_html_codec, remove_html_tags


def clean_web_text(text: str) -> str:
    """
    Function for cleaning web text.
    :param text: Text to clean.
    :return: Cleaned text.
    """
    return remove_html_tags(clean_html_codec(text))


class ScrapingModule(object):
    """
    Class, representing a scraping module.
    """

    def __init__(self, target_pages: List[str], target_entry: str, entry_callback: Any) -> None:
        """
        Initiation method.
        :param target_pages: Target pages.
        :param target_entry: Target entry name.
        :param entry_callback: Callback function for writing entry data to storage.
        """
        self.target_pages = target_pages
        self.target_entry = target_entry
        self.entry_callback = entry_callback

    @abstractmethod
    def active(self, page_url: str, page_content: html.HtmlElement) -> bool:
        """
        Method for checking active status on page content.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping module is active on given page, else False.
        """
        pass

    @abstractmethod
    def scrape(self, page_url: str, page_content: html.HtmlElement) -> None:
        """
        Method for scraping target entry data from side.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        """
        pass


class TableScrapingModule(ScrapingModule):
    """
    Class, representing a table scraping module for SAP table data.
    """

    def __init__(self, target_pages: List[str], target_entry: str, entry_callback: Any) -> None:
        """
        Initiation method.
        :param target_pages: Target pages.
        :param target_entry: Target entry name.
        :param entry_callback: Callback function for writing entry data to storage.
            A callback function should take the target entity as first, the entity data (dictionary) as second argument.
        """
        target_pages = [urlparse(url).netloc for url in target_pages]
        print(target_pages)
        super().__init__(target_pages, target_entry, entry_callback)

        self.collection_dicts = {
            "www.se80.co.uk": {
                "name": "//header/title/text()",
                "content": "//div[@id='wrapper']//div[@class='pageContent']/h2[contains(./text(), ' data')]/text()",
                "description": "//div[@id='wrapper']//div[@class='pageContent']/p/text()"
            }

        }
        self.cleaning_dicts = {
            "www.se80.co.uk": {
                "name": lambda x: x[0].split(" SAP (")[0] if x else None,
                "content": lambda x: x[0].split(" data")[0] if x else None,
                "description": lambda x: "\n".join([clean_web_text(elem) for elem in x]) if x else None
            }

        }

    def active(self, page_url: str, page_content: html.HtmlElement) -> bool:
        """
        Method for checking active status on page content.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping module is active on given page, else False.
        """
        parsed_url = urlparse(page_url)
        if parsed_url.netloc in self.target_pages:
            if parsed_url.netloc == "www.se80.co.uk" and "/sap-tables/" in page_url:
                return True
        return False

    def scrape(self, page_url: str, page_content: html.HtmlElement) -> None:
        """
        Method for scraping target entry data from side.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        """
        data = {}
        if "www.se80.co.uk" in page_url:
            source = "www.se80.co.uk"

        data = requests_utility.safely_collect(
            page_content, self.collection_dicts[source], self.cleaning_dicts[source])
        data["url"] = page_url

        if source == "www.se80.co.uk":
            if data["name"] is None and "/sap-tables/?name=" in page_url:
                data["name"] = page_url.split("/sap-tables/?name=")[1]
            metadata = {}

            fields = {"keys": [], "non-keys": []}
            tables = page_content.xpath(
                "//div[@id='wrapper']//div[@class='pageContent']//table")
            key_fields_table = tables[0]
            table_fields_table = tables[1]
            key_columns = key_fields_table.xpath(
                ".//tr[@class='headField']/td/b/text()")
            table_columns = table_fields_table.xpath(
                ".//tr[@class='headField']/td/b/text()")

            for row in key_fields_table.xpath("./tr[not(contains(./@class, 'headField'))]"):
                values = [elem.xpath("./a")[0].text if elem.xpath("./a")
                          else elem.text for elem in row.xpath("./td")]
                fields["keys"].append({
                    key_column: values[column_index] for column_index, key_column in enumerate(key_columns)
                })
            for row in table_fields_table.xpath("./tr[not(contains(./@class, 'headField'))]"):
                values = [elem.xpath("./a")[0].text if elem.xpath("./a")
                          else elem.text for elem in row.xpath("./td")]
                fields["non-keys"].append({
                    non_key_column: values[column_index] for column_index, non_key_column in enumerate(table_columns)
                })

            data["meta_data"] = metadata
            data["fields"] = fields
        self.entry_callback(self.target_entry, data)
