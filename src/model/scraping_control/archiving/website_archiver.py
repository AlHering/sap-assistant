# -*- coding: utf-8 -*-
"""
****************************************************
*                  ScrapingService                 
*            (c) 2022 Alexander Hering             *
****************************************************
"""
import os
import hashlib
from abc import ABC, abstractmethod
from urllib.parse import urlparse
import requests
from typing import Optional, Any, List, Union, Tuple
from src.configuration import configuration as cfg
from src.utility.bronze import sqlalchemy_utility, dictionary_utility
from src.utility.silver import internet_utility
from requests.exceptions import SSLError
from src.model.scraping_control.archiving import website_archiver_database
from src.model.scraping_control import media_metadata


# TODO: Basic wget-Archiver via "wget --mirror --page-requisites --convert-link --no-clobber --no-parent --domains [domains] [URL]"


class WebsiteArchiver(ABC):
    """
    General Website Archiver class.
    """

    def __init__(self, profile: dict) -> None:
        """
        Initiation method for Website Archiver objects.
        :param profile: Archiver profile.
            'base_url': Starting URL for archiver.
            'allowed_bases': Optional. Defines allowed URL bases to limit crawled content. Base of 'base_url' is used,
                if 'allowed_bases' is not configured.
            'framework_arguments': Optional. Framework arguments to forward to Archiver framework engine
                - session creation in case of requests
                - driver creation in case of selenium
                - spider configuration in case of scrapy
            'offline_copy_path': Optional. Results in the creation of an offline copy with the given path as root
                folder.
        """
        self.logger = cfg.LOGGER
        self.logger.info(
            f"Initializing WebsiteArchiver {self} with profile: {profile}")
        # Handling data backend
        self.database = website_archiver_database
        self.website_entry = self.database.get_or_create_website_entry(profile)
        self.website_id = self.website_entry.id
        self.media_handler = media_metadata.MediaMetadata()
        self.media_metadata = self.media_handler.media

        # Handle archiver instance variables
        self.profile = profile
        self.offline_copy_path = profile.get("offline_copy_path")
        if self.offline_copy_path is not None and not os.path.exists(self.offline_copy_path):
            os.makedirs(self.offline_copy_path)
        self.base_url = profile["base_url"]
        self.allowed_bases = profile.get("allowed_bases")
        self.base_url_base = urlparse(self.base_url).netloc
        self.allowed_bases = self.allowed_bases if self.allowed_bases is not None else [
            self.base_url_base]
        self.crawled_pages = []
        self.crawled_assets = []

        self._cache = {}

    @abstractmethod
    def archive_website(self, *args: Optional[Any], **kwargs: Optional[Any]) -> None:
        """
        Abstract method for archiving website.
        :param args: Arbitrary arguments.
        :param kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def create_state_dump(self, *args: Optional[Any], **kwargs: Optional[Any]) -> None:
        """
        Abstract method for creating state dump of archiver.
        :param args: Arbitrary arguments.
        :param kwargs: Arbitrary keyword arguments.
        """
        pass

    @abstractmethod
    def load_state_dump(self, *args: Optional[Any], **kwargs: Optional[Any]) -> None:
        """
        Abstract method for loading state dump of archiver.
        :param args: Arbitrary arguments.
        :param kwargs: Arbitrary keyword arguments.
        """
        pass

    def register_page(self, page_url: str, page_content: Union[str, bytes] = None, offline_path: str = None) -> None:
        """
        Method for registering a page.
        :param page_url: Page URL.
        :param page_content: Page content. Defaults to None.
        :param offline_path: Offline path. Defaults to None in which case offline path is created dynamically if
            'offline_copy_path' is given in profile.
        """
        self.logger.info(f"Registering page '{page_url}'")
        if page_content is not None and self.offline_copy_path is not None:
            if offline_path is None:
                offline_path = self.convert_url_to_path(page_url)
            if isinstance(page_content, str):
                open(offline_path, "w", encoding="utf-8").write(page_content)
            elif isinstance(page_content, bytes):
                open(offline_path, "wb").write(page_content)
        self.database.register_page(
            self.website_id, page_url, page_content, offline_path)

    def register_temporary_page_links(self, source_url: str, page_links: List[str]) -> None:
        """
        Method for registering temporary page links.
        :param source_url: Source page URL.
        :param page_links: Target links.
        """
        self.logger.info(
            f"Registering {len(page_links)} temporary page links '{source_url}'")
        self.database.register_temporary_page_links(
            self.website_id, source_url, page_links)

    def register_asset(self, source_url: str, asset_url: str, asset_type: str, asset_content: bytes = None,
                       asset_encoding: str = None, asset_extension: str = None, offline_path: str = None) -> None:
        """
        Method for registering an asset.
        :param source_url: Source page URL.
        :param asset_url: Asset URL.
        :param asset_type: Asset type.
        :param asset_content: Asset content. Defaults to None.
        :param asset_encoding: Asset encoding.
        :param asset_extension: Asset extension
        :param offline_path: Offline path. Defaults to None in which case offline path is created dynamically if
            'offline_copy_path' is given in profile.
        """
        self.logger.info(
            f"Registering asset '{asset_url}' under '{source_url}'")
        self.logger.info(
            f"Metadata for '{asset_url}': '{asset_type}', '{asset_encoding}', '{asset_extension}'")
        if asset_content is not None and self.offline_copy_path is not None:
            if offline_path is None:
                offline_path = self.convert_url_to_path(
                    asset_url, asset_extension if asset_extension else ".html")
            open(offline_path, "wb").write(asset_content)
        self.database.register_asset(self.website_id, source_url, asset_url, asset_type if asset_type is not None else "unkown", str(asset_content),
                                     asset_encoding, asset_extension, offline_path)

    def register_link(self, source_url: str, target_url: str, target_type: str) -> None:
        """
        Method for creating or updating links.
        :param source_url: Source page URL.
        :param target_url: Target URL.
        :param target_type: Target type: Either 'page' or 'asset'.
        """
        self.logger.info(
            f"Registering link '{target_url}' ({target_type}) under '{source_url}'")
        self.database.register_link(
            self.website_id, source_url, target_url, target_type)

    def fix_link(self, current_url: str, link: str) -> str:
        """
        Method for fixing partial links.
        :param current_url: Current URL.
        :param link: Link to fix.
        :return: Fixed link.
        """
        parsed_link = urlparse(link)
        if not parsed_link.scheme and not parsed_link.netloc:
            parsed_base = urlparse(current_url)
            link = f"{parsed_base.scheme}://{parsed_base.netloc}/{link if not link.startswith('/') else link[1:]}"
        return link

    def get_asset_data(self, asset_url: str) -> Tuple[str, bytes, str, str]:
        """
        Method for retrieving asset data from url.
        :param asset_url: Asset URL.
        :return: Tuple of asset type, asset content, asset encoding and asset extension.
        """
        try:
            asset_head = self._cache.get(
                "session", requests).head(asset_url).headers
            asset = self._cache.get("session", requests).get(
                asset_url, stream=True)
        except SSLError:
            asset_head = self._cache.get("session", requests).head(
                asset_url, verify=False).headers
            asset = self._cache.get("session", requests).get(
                asset_url, stream=True, verify=False)

        asset_type = asset_head.get("Content-Type")
        main_type, sub_type = asset_head.get(
            "Content-Type", "/").lower().split("/")
        asset_encoding = asset.apparent_encoding if hasattr(
            asset, "apparent_encoding") else asset.encoding
        asset_content = asset.content
        asset_extension = self.media_metadata.get(
            main_type, {}).get(sub_type, {}).get("extension")
        return asset_type, asset_content, asset_encoding, asset_extension

    def convert_url_to_path(self, url: str, extension: str = ".html") -> str:
        """
        Method for converting URL to path.
        :param url: URL to convert
        :param extension: Extension for asset. Defaults to '.html'.
        :return: Path for given URL.
        """
        parsed_url = urlparse(url)
        path_parts = (parsed_url.netloc + parsed_url.path).split("/")

        while "" in path_parts:
            path_parts.remove("")
        if len(path_parts) == 0:
            path_parts.append(
                f"{str(hashlib.md5(url.encode()).hexdigest())}.html")

        path_parts = os.path.join(self.offline_copy_path, *path_parts)
        path_directory = os.path.abspath(
            os.path.join(path_parts, os.path.pardir))
        if not os.path.isdir(path_directory):
            os.makedirs(path_directory)

        _, ext = os.path.splitext(path_parts)
        if not ext or ext in parsed_url.netloc:
            path_parts = f"{path_parts}{extension}"
        return path_parts

    def relink_temporary_links(self) -> None:
        """
        Method for checking and adjusting temporary links.
        Internal links are extracted from external link table and internal links are created off of them.
        """
        self.database.relink_temporary_links(self.website_id)
