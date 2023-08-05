# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
from typing import Any, List, Tuple, Optional
from datetime import datetime as dt
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility
from src.utility.silver import file_system_utility
# from src.control.plugin_controller import PluginController


# TODO: Implement cache for efficiency optimization
# TODO: Handle inactivity flag
# TODO: Implement plugin support

class WebsiteFilestore(object):
    """
    Class, representing website fielstore.
    """

    def __init__(self, working_directory: str = None, schema: str = "", verbose: bool = False) -> None:
        """
        Initiation method.
        :param working_directory: Working directory.
            Defaults to None in which case the central WEBSITE_ARCHIVER_FOLDER ENV variable is used.
        :param schema: Schema to use.
            Defaults to empty string in which case no schema is used.
        :param verbose: Verbose flag for interaction methods.
            Defaults to False since archiver is already logging.
        """
        self._logger = cfg.LOGGER
        self.verbose = verbose
        self._logger.info("Automapping existing structures")
        self.working_directory = cfg.ENV["WEBSITE_ARCHIVER_FOLDER"] if working_directory is None else working_directory
        self.schema = schema
        if self.schema and not os.path.exists(os.path.join(self.working_directory, self.schema)):
            self.working_directory = os.path.join(
                self.working_directory, self.schema)
        self.index_path = os.path.join(self.working_directory, "index.json")
        self.index = {
            "pages": 0,
            "assets": 0,
            "next_urls": []
        }
        self._initiate_infrastructure()

    """
    Basic setup
    """

    def _initiate_infrastructure(self) -> None:
        """
        Metthod for generating archiving tables.
        """
        self._logger.info(
            f"Generating archiving tables for website with schema {self.schema}")
        file_system_utility.safely_create_path(self.working_directory)
        if os.path.exists(self.index_path):
            self.index = json_utility.load(self.index_path)

    """
    Interfacing methods
    """

    def register_page(self, page_url: str, page_content: str = None,
                      page_path: str = None) -> None:
        """
        Method for creating or updating links.
        :param self.schema: Website ID.
        :param page_url: Page URL.
        :param page_content: Page content. Defaults to None.
        :param page_path: Page path. Defaults to None
        """
        if self.verbose:
            self._logger.info(
                f"Registering page for website {self.schema}: {page_url}")
        content_path = file_system_utility.clean_directory_name(page_url)
        data_path = content_path + "_data.json"
        if not os.path.exists(data_path):
            json_utility.save({
                "page_id": self.index["pages"],
                "page_url": page_url,
                "created": dt.now(),
                "updated": dt.now(),
                "inactive": False
            },
                data_path)
            self.index["pages"] += 1
        else:
            data = json_utility.load(data_path)
            if data["inactive"]:
                data["inactive"] = False
                data["updated"] = dt.now()
                json_utility.save(data, data_path)

        if page_content is not None and not os.path.exists(content_path):
            open(content_path, "w", encoding="utf-8").write(page_content)

    def register_asset(self, source_url: str, asset_url: str, asset_type: str, asset_content: str = None,
                       asset_encoding: str = None, asset_extension: str = None, asset_path: str = None) -> None:
        """
        Method for creating or updating links.
        :param self.schema: Website ID.
        :param source_url: Source page URL.
        :param asset_url: Asset URL.
        :param asset_type: Asset type.
        :param asset_content: Asset content. Defaults to None.
        :param asset_encoding: Asset encoding.
        :param asset_extension: Asset extension.
        :param asset_path: Asset path. Defaults to None
        """
        if self.verbose:
            self._logger.info(
                f"Registering asset for website {self.schema}: {asset_url}")

        content_path = file_system_utility.clean_directory_name(
            asset_url) if asset_path is None else asset_path
        data_path = content_path + "_data.json"

        if not os.path.exists(data_path):
            json_utility.save({
                "asset_id": self.index["assets"],
                "asset_type": asset_type,
                "asset_url": asset_url,
                "created": dt.now(),
                "updated": dt.now(),
                "inactive": False
            },
                data_path)
            self.index["assets"] += 1
        else:
            data = json_utility.load(data_path)
            if data["inactive"]:
                data["inactive"] = False
                data["updated"] = dt.now()
                json_utility.save(data, data_path)

        if not (asset_extension is None or content_path.endswith(asset_extension)):
            content_path += asset_extension
        if asset_content is not None and not os.path.exists(content_path):
            open(content_path, "wb",
                 encoding="utf-8" if asset_encoding is None else asset_encoding).write(asset_content)

    def register_link(self, source_url: str, target_url: str, target_type: str) -> bool:
        """
        Method for creating or updating links.
        :param self.schema: Website ID.
        :param source_url: Source page URL.
        :param target_url: Target URL.
        :param target_type: Target type: Either 'page' or 'asset'.
        :return: Flag, declaring whether link was already registered.
        """
        if self.verbose:
            self._logger.info(
                f"Registering link for website {self.schema}: {source_url} -> {target_url} ({target_type})")
        link_path = file_system_utility.clean_directory_name(
            source_url) + "_links.json"
        if not os.path.exists(link_path):
            data = {
                "asset": [],
                "page": []
            }
        else:
            data = json_utility.load(link_path)
        if target_url not in data[target_type]:
            data[target_type].append(target_url)
            json_utility.save(data, link_path)
            if target_type == "page":
                target_data_path = file_system_utility.clean_directory_name(
                    target_url) + "_data.json"
                if not os.path.exists(target_data_path):
                    self.index["next_urls"].append(target_url)
            return True
        else:
            return False

    def get_element_count(self) -> Tuple[int, int]:
        """
        Method for creating or updating links.
        :param self.schema: Website ID.
        :return: Tuple of the numbers of tracked pages and assets.
        """
        if self.verbose:
            self._logger.info(
                f"Counting {self.schema}'s tracked elements...")
        page_count = self.index["pages"]
        asset_count = self.index["assets"]
        if self.verbose:
            self._logger.info(
                f"Counted {page_count} pages and {asset_count} assets under {self.schema}'s tracked elements.")
        return page_count, asset_count

    def get_next_url(self, page_url: str) -> Optional[str]:
        """
        Method for marking current URL as visited and retrieving next target URL.
        :param self.schema: Website ID.
        :param page_url: Current URL.
        :return: Next target URL if found, else None.
        """
        if self.verbose:
            self._logger.info(f"Finished {self.schema}: {page_url}")

        while page_url in self.index["next_urls"]:
            self.index["next_urls"].remove(page_url)

        return self.index["next_urls"][0] if self.index["next_urls"] else None

    def check_for_existence(self, url: str, target_type: str) -> bool:
        """
        Method for marking current URL as visited and retrieving next target URL.
        :param self.schema: Website ID.
        :param url: Target URL.
        :param target_type: Target type: Either 'page' or 'asset'.
        :return: Flag, declaring whether target was already registered.
        """
        if self.verbose:
            self._logger.info(
                f"Checking for existence {self.schema}: {url} ({target_type})")
        data_path = file_system_utility.clean_directory_name(
            url) + "_data.json"

        return os.path.exists(data_path)
