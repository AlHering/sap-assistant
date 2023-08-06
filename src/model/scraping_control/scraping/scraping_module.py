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
    return remove_html_tags(clean_html_codec(str))


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
    def scrape(self, page_url: str, page_content: html.HtmlElement) -> bool:
        """
        Method for scraping target entry data from side.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping process was sucessful, else False.
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
        """
        target_pages = [urlparse(url).netloc for url in target_pages]
        super().__init__(target_pages, target_entry, entry_callback)

        self.collection_dicts = {
            "se80.co.uk": {
                "name": "//header/title/text()",
                "content": ["//div[@id='wrapper']//div[@class='pageContent']/h2[contains(./text(), ' data')]"],
                "description": ["//div[@id='wrapper']//div[@class='pageContent']/p/text()"]

            }

        }
        self.cleaning_dicts = {
            "se80.co.uk": {
                "name": lambda x: x[0].split(" SAP (")[0] if x else None,
                "content": lambda x: x[0].split(" data")[0] if x else None,
                "description": lambda x: "\n".join([clean_web_text(elem) for elem in x]) if x else None,
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
            if parsed_url.netloc == "se80.co.uk" and "/sap-tables/" in page_url:
                return True
        return False

    def scrape(self, page_url: str, page_content: html.HtmlElement) -> bool:
        """
        Method for scraping target entry data from side.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping process was sucessful, else False.
        """
        data = {}
        try:
            data["url"] = page_url

            return True
        except:
            return False
