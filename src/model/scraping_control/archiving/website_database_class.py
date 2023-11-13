# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
import copy
from sqlalchemy import Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, func, select
from sqlalchemy.ext.automap import automap_base
from typing import Any, List, Tuple, Optional
import datetime
from src.configuration import configuration as cfg
from src.utility.bronze import sqlalchemy_utility
from src.utility.gold.basic_sqlalchemy_interface import BasicSQLAlchemyInterface, FilterMask
from src.model.scraping_control.archiving.archiver_data_model import populate_data_instrastructure
# from src.control.plugin_controller import PluginController


# TODO: Implement target masking for efficiency optimization
# TODO: Implement architecture and block extraction for website analyzation purposes
# TODO: Implement plugin support

class WebsiteDatabase(BasicSQLAlchemyInterface):
    """
    Class, representing website database.
    """

    def __init__(self, database_uri: str = None, schema: str = None, verbose: bool = False) -> None:
        """
        Initiation method.
        :param database_uri: Database URI.
            Defaults to None in which case the central WEBSITE_ARCHIVER_DB ENV variable is used.
        :param schema: Schema to use.
            Defaults to None in which case no schema is used.
        :param verbose: Verbose flag for interaction methods.
            Defaults to False since archiver is already logging.
        """
        working_directory = os.path.join(
            cfg.PATHS.DATA_PATH, "archiving", "schema" if schema else "website_database")
        self.run = None
        if not schema.endswith("."):
            schema += "."
        super().__init__(working_directory=working_directory,
                         database_uri=database_uri, population_function=populate_data_instrastructure,
                         schema=schema, logger=cfg.LOGGER)
        self.verbose = verbose
        self.base.prepare(autoload_with=self.engine, reflect=True)
        self._logger.info("base created with")
        self._logger.info(f"Classes: {self.base.classes.keys()}")
        self._logger.info(f"Tables: {self.base.metadata.tables.keys()}")

    """
    Interfacing methods
    """

    def set_run(self, profile: dict, reload: bool) -> dict:
        """
        Method for setting run.
        :param profile: Profile for the current run.
        :param reload: Flag for declaring whether to reload last unfinished run.
        """
        last_run = self.get_objects_by_filtermasks(
            f"{self.schema}runs", [FilterMask([["profile", "==", profile]])])[-1]
        if last_run.finished is None and reload:
            self.run = last_run
        else:
            run_id = self.post_object(
                f"{self.schema}runs", profile=profile, cache={})
            self.run = self.get_object_by_id(f"{self.schema}runs", run_id)

    def get_cache(self) -> dict:
        """
        Method for getting the cache.
        """
        return self.run.cache

    def update_cache(self, cache: dict) -> None:
        """
        Method for updating the cache.
        :param cache: Cache update.
        """
        self.run.cache = copy.deepcopy(cache)
        self.patch_object(f"{self.schema}runs", self.run.run_id, cache=cache)

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
        with self.session_factory() as session:
            page = session.query(self.model[f"{self.schema}pages"]).filter(
                self.model[f"{self.schema}pages"].page_url == page_url
            ).first()
            if page is None:
                if self.verbose:
                    self._logger.info(
                        f"Found already registered page for website {self.schema}: {page_url}")
                page = self.model[f"{self.schema}pages"](
                    page_url=page_url, created=datetime.datetime.now(), inactive="")
                session.add(page)
            elif page.inactive != "":
                page.inactive = ""

            page.updated = datetime.datetime.now()
            session.commit()
            session.refresh(page)

            # Create or update raw page entry, if existing
            if page_content is not None or page_path is not None:
                raw_pages = session.query(self.model[f"{self.schema}raw_pages"]).filter(
                    self.model[f"{self.schema}raw_pages"].page_id == page.page_id
                ).all()
                for raw_page in raw_pages:
                    if raw_page.inactive == "":
                        raw_page.inactive = "x"
                        raw_page.updated = datetime.datetime.now()
                new_raw_page = self.model[f"{self.schema}raw_pages"](
                    page_id=page_url, created=datetime.datetime.now())
                if page_content is not None:
                    new_raw_page.raw = page_content
                if page_path is not None:
                    new_raw_page.path = page_path
                session.add(new_raw_page)
            session.commit()

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
        with self.session_factory() as session:
            asset = session.query(self.model[f"{self.schema}assets"]).filter(
                self.model[f"{self.schema}assets"].asset_url == asset_url
            ).first()
            if asset is None:
                asset = self.model[f"{self.schema}assets"](
                    asset_url=asset_url, asset_type=asset_type, created=datetime.datetime.now())
                session.add(asset)
            elif asset.inactive != "":
                if self.verbose:
                    self._logger.info(
                        f"Found already registered inactivate asset for website {self.schema}: {asset_url}")
                asset.inactive = ""
            else:
                if self.verbose:
                    self._logger.info(
                        f"Found already registered asset for website {self.schema}: {asset_url}")

            asset.updated = datetime.datetime.now()
            session.commit()
            session.refresh(asset)

            # Create or update raw asset entry, if existing
            if asset_content is not None or asset_path is not None:
                raw_assets = session.query(self.model[f"{self.schema}raw_assets"]).filter(
                    self.model[f"{self.schema}raw_assets"].asset_id == asset.asset_id
                ).all()
                for raw_asset in raw_assets:
                    if raw_asset.inactive == "":
                        raw_asset.inactive = "x"
                        raw_asset.updated = datetime.datetime.now()
                new_raw_asset = self.model[f"{self.schema}raw_assets"](
                    asset_id=asset.asset_id,
                    created=datetime.datetime.now()
                )
                if asset_content is not None:
                    new_raw_asset.raw = asset_content
                    new_raw_asset.encoding = asset_encoding
                    new_raw_asset.extension = asset_extension
                if asset_path is not None:
                    new_raw_asset.path = asset_path
                session.add(new_raw_asset)

            session.commit()

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
        target_column = getattr(
            self.model[f"{self.schema}{target_type}_network"], f"target_{target_type}_url")
        link = None
        with self.session_factory() as session:
            link = session.query(self.model[f"{self.schema}{target_type}_network"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    self.model[f"{self.schema}{target_type}_network"].source_page_url == source_url,
                    target_column == target_url
                )
            ).first()
            if link is None:
                creation_kwargs = {
                    "source_page_url": source_url,
                    f"target_{target_type}_url": target_url,
                    "created": datetime.datetime.now()
                }
                if target_type == "page":
                    creation_kwargs["followed"] = False
                session.add(self.model[f"{self.schema}{target_type}_network"](
                    **creation_kwargs
                ))
            else:
                if self.verbose:
                    self._logger.info(
                        f"Found already registered link for {source_url} -> {target_url}")
                link.inactive = ""
                link.updated = datetime.datetime.now()
            session.commit()
            return link is None

    def get_element_count(self) -> Tuple[int, int]:
        """
        Method for creating or updating links.
        :param self.schema: Website ID.
        :return: Tuple of the numbers of tracked pages and assets.
        """
        if self.verbose:
            self._logger.info(
                f"Counting {self.schema}'s tracked elements...")
        page_count = int(self.engine.connect().execute(select(func.count()).select_from(
            self.model[f"{self.schema}pages"])).scalar())
        asset_count = int(self.engine.connect().execute(select(func.count()).select_from(
            self.model[f"{self.schema}assets"])).scalar())
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
        next_link = None
        with self.session_factory() as session:
            followed = session.query(self.model[f"{self.schema}page_network"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    self.model[f"{self.schema}page_network"].followed == False,
                    self.model[f"{self.schema}page_network"].target_page_url == page_url)
            ).all()
            for entry in followed:
                entry.followed = True
                entry.updated = datetime.datetime.now()
            session.commit()
            if self.verbose:
                self._logger.info(f"Updated {self.schema}: {page_url} links")

            while next_link is None:
                next_link = session.query(self.model[f"{self.schema}page_network"]).filter(
                    self.model[f"{self.schema}page_network"].followed == False
                ).first()
                if next_link is None:
                    break

                alredy_visited = session.query(self.model[f"{self.schema}page_network"]).filter(
                    sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                        self.model[f"{self.schema}page_network"].followed == True,
                        self.model[f"{self.schema}page_network"].target_page_url == next_link.target_page_url)
                ).first()
                if alredy_visited is not None:
                    for alredy_visited in session.query(self.model[f"{self.schema}page_network"]).filter(
                        sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                            self.model[f"{self.schema}page_network"].followed == False,
                            self.model[f"{self.schema}page_network"].target_page_url == next_link.target_page_url)
                    ).all():
                        alredy_visited.followed = True
                        alredy_visited.updated = datetime.datetime.now()
                    session.commit()
                    next_link = None
                else:
                    next_link = next_link.target_page_url
        return next_link

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
        found = False
        url_column = getattr(
            self.model[f"{self.schema}{target_type}s"], f"{target_type}_url")
        inactive_column = getattr(
            self.model[f"{self.schema}{target_type}s"], f"inactive")
        with self.session_factory() as session:
            entry = session.query(self.model[f"{self.schema}page_network"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    url_column == False,
                    inactive_column == "")
            ).first()
            found = entry is not None
        return found
