# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from abc import ABC, abstractmethod
from sqlalchemy import Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, func, select
from sqlalchemy.ext.automap import automap_base
from typing import Any, List, Tuple, Optional
import datetime
from src.configuration import configuration as cfg
from src.utility.bronze import sqlalchemy_utility


class ScrapingModule(object):
    """
    Class, representing a scraping module.
    """

    def __init__(self, taget_pages: List[str], target_entry: str, entry_callback: Any) -> None:
        """
        Initiation method.
        :param taget_pages: Target pages.
        :param target_entry: Target entry name.
        :param entry_callback: Callback function for writing entry data to storage.
        """
        self.target_pages = taget_pages
        self.target_entry = target_entry
        self.entry_callback = entry_callback

    @abstractmethod
    def active(self, page_url: str, page_content: str) -> bool:
        """
        Method for checking active status on page content.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping module is active on given page, else False.
        """
        pass

    @abstractmethod
    def scrape(self, page_url: str, page_content: str) -> bool:
        """
        Method for scraping target entry data from side.
        :param page_url: Current page URL.
        :param page_content: Current page content.
        :return: True, if scraping process was sucessful, else False.
        """
        pass
