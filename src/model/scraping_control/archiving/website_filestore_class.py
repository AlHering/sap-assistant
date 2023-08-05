from sqlalchemy import MetaData, Table, Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, Table, \
    Float, BLOB, TEXT, func, inspect, select, text
from sqlalchemy import and_, or_, not_
from sqlalchemy.ext.automap import automap_base, classname_for_table
from typing import Any, Union, List, Tuple, Optional
import copy
import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from src.configuration import configuration as cfg
from src.utility.bronze import dictionary_utility, sqlalchemy_utility, time_utility
import logging
from src.control.plugin_controller import PluginController


# TODO: Implement target masking for efficiency optimization
# TODO: Implement architecture and block extraction for website analyzation purposes

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
        self.base = automap_base()
        self.working_directory = cfg.ENV["WEBSITE_ARCHIVER_FOLDER"] if working_directory is None else working_directory
        self.base.prepare(autoload_with=self.engine, reflect=True)
        self._logger.info("base created with")
        self._logger.info(f"Classes: {self.base.classes.keys()}")
        self._logger.info(f"Tables: {self.base.metadata.tables.keys()}")

        self.model = None
        self.session_factory = None
        self.schema = schema
        if self.schema and not self.schema.endswith("."):
            self.schema += "."
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
        self.schema = str(self.schema)

        class Run(self.base):
            """
            Page dataclass, representing a scraping run of a website.
            """
            __tablename__ = f"{self.schema}runs"
            __table_args__ = {
                "comment": "Website Run Table.", "extend_existing": True}

            run_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                            comment="ID of the run.")
            profile = Column(JSON, nullable=True,
                             comment="Profile of run.")

            started = Column(DateTime, default=func.now(),
                             comment="Starting timestamp.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            finished = Column(DateTime, nullable=True,
                              comment="Finishing timestamp.")

        class Page(self.base):
            """
            Page dataclass, representing a page of a website.
            """
            __tablename__ = f"{self.schema}pages"
            __table_args__ = {
                "comment": "Website Page Table.", "extend_existing": True}

            page_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of the page.")
            page_url = Column(Text, nullable=False, unique=True,
                              comment="URL of page.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class Asset(self.base):
            """
            Page dataclass, representing an asset of a website.
            """
            __tablename__ = f"{self.schema}assets"
            __table_args__ = {
                "comment": "Website Asset Table.", "extend_existing": True}

            asset_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                              comment="ID of the asset.")
            asset_type = Column(String, nullable=False,
                                comment="Type of the asset.")
            asset_url = Column(Text, nullable=False, unique=True,
                               comment="URL of Asset.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class PageLink(self.base):
            """
            Page dataclass, representing the page network of a website.
            """
            __tablename__ = f"{self.schema}page_network"
            __table_args__ = {
                "comment": "Website Page Network Table.", "extend_existing": True}

            link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a network link.")
            source_page_url = Column(Text, ForeignKey(f"{self.schema}pages.page_url"), nullable=False,
                                     comment="Source page URL of the network link.")
            target_page_url = Column(Text, ForeignKey(f"{self.schema}pages.page_url"), nullable=False,
                                     comment="Target page URL of the network link.")

            followed = Column(Boolean, nullable=False, default=False,
                              comment="Flag declaring whether page link was followed.")
            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class ExternalPageLink(self.base):
            """
            Page dataclass, representing the external page network of a website.
            """
            __tablename__ = f"{self.schema}external_page_network"
            __table_args__ = {
                "comment": "Website External Page Network Table.", "extend_existing": True}

            link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a network link.")
            source_page_url = Column(Text, ForeignKey(f"{self.schema}pages.page_url"), nullable=False,
                                     comment="Source page URL of the network link.")
            target_page_url = Column(Text, nullable=False,
                                     comment="Target page URL.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class AssetLink(self.base):
            """
            Page dataclass, representing the asset network of a website.
            """
            __tablename__ = f"{self.schema}asset_network"
            __table_args__ = {
                "comment": "Website Asset Network Table.", "extend_existing": True}

            link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a network link.")
            source_page_url = Column(Text, ForeignKey(f"{self.schema}pages.page_url"), nullable=False,
                                     comment="Source page URL of the network link.")
            target_asset_url = Column(Text, ForeignKey(f"{self.schema}assets.asset_url"), nullable=False,
                                      comment="Target asset URL of the network link.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class Block(self.base):
            """
            Page dataclass, representing a block of a website.
            """
            __tablename__ = f"{self.schema}blocks"
            __table_args__ = {
                "comment": "Website Block Table.", "extend_existing": True}

            block_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                              comment="ID of a network link.")
            element_count = Column(Integer, nullable=True,
                                   comment="Element count of a website block.")
            link_count = Column(Integer, nullable=True,
                                comment="Link count of a website block.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class Architecture(self.base):
            """
            Page dataclass, representing an architecture instance of a website.
            """
            __tablename__ = f"{self.schema}architecture"
            __table_args__ = {
                "comment": "Website Architecture Table.", "extend_existing": True}

            instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                                 comment="ID of an architecture instance.")
            page_id = Column(Integer, ForeignKey(f"{self.schema}pages.page_id"), nullable=False,
                             comment="Page ID of the architecture instance.")
            block_id = Column(Integer, ForeignKey(f"{self.schema}blocks.block_id"), nullable=False,
                              comment="Block ID of the architecture instance.")
            start_element = Column(Integer, nullable=True,
                                   comment="Start element of the block.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class RawPage(self.base):
            """
            Page dataclass, representing a raw page of a website.
            """
            __tablename__ = f"{self.schema}raw_pages"
            __table_args__ = {
                "comment": "Website Raw Page Table.", "extend_existing": True}

            instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                                 comment="ID of a raw page instance.")
            page_id = Column(Integer, ForeignKey(f"{self.schema}pages.page_id"), nullable=False,
                             comment="Page ID of the instance.")
            raw = Column(Text, nullable=True,
                         comment="Raw content of the page.")
            path = Column(Text, nullable=True,
                          comment="Path to the current offline copy of the page.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class RawAsset(self.base):
            """
            Page dataclass, representing a raw asset of a website.
            """
            __tablename__ = f"{self.schema}raw_assets"
            __table_args__ = {
                "comment": "Website Raw Asset Table.", "extend_existing": True}

            instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                                 comment="ID of a raw asset instance.")
            asset_id = Column(Integer, ForeignKey(f"{self.schema}assets.asset_id"), nullable=False,
                              comment="Asset ID of the instance.")
            raw = Column(Text, nullable=True,
                         comment="Raw content of the asset.")
            encoding = Column(String, nullable=True,
                              comment="Target encoding of the asset.")
            extension = Column(String, nullable=True,
                               comment="Target extension of the asset.")
            path = Column(Text, nullable=True,
                          comment="Path to the current offline copy of the asset.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        for dataclass in [Run, Page, Asset, PageLink, ExternalPageLink, AssetLink, Block, Architecture, RawPage, RawAsset]:
            self.model[dataclass.__tablename__] = dataclass
        if self.verbose:
            self._logger.info(f"self.model after addition: {self.model}")
        self._logger.info("Creating new structures")
        self.base.metadata.create_all(bind=self.engine)

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

            # Create or update raw page entry, if existing
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

            # Handling registration of link
            if source_url is not None:
                source_page = session.query(self.model[f"{self.schema}pages"]).filter(
                    sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                        self.model[f"{self.schema}pages"].page_url == source_url)
                ).first()
                if source_page.inactive != "":
                    source_page.updated = datetime.datetime.now()
                    source_page.inactive = ""

                link = session.query(self.model[f"{self.schema}asset_network"]).filter(
                    sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                        self.model[f"{self.schema}asset_network"].source_page_url == source_page.page_url,
                        self.model[f"{self.schema}asset_network"].target_asset_url == asset.asset_url
                    )
                ).first()
                if link is None:
                    link = self.model[f"{self.schema}asset_network"](
                        source_page_url=source_page.page_url,
                        target_asset_url=asset.asset_url,
                        created=datetime.datetime.now()
                    )
                    session.add(link)
                elif link.inactive != "":
                    link.inactive = ""
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
