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


# TODO: Modularize database interaction
LOGGER = cfg.LOGGER
LOGGER.info("Automapping existing structures")
BASE = automap_base()
ENGINE = sqlalchemy_utility.get_engine(cfg.ENV["WEBSITE_ARCHIVER_DB"])
BASE.prepare(autoload_with=ENGINE, reflect=True)
LOGGER.info("Base created with")
LOGGER.info(f"Classes: {BASE.classes.keys()}")
LOGGER.info(f"Tables: {BASE.metadata.tables.keys()}")


"""
Dataclasses & Setup
"""
LOGGER.info("Setting up new infrastructure")
Website = None
if "website" not in BASE.classes:
    LOGGER.info("Website class is not declared yet, rebuilding base.")
    BASE = declarative_base()

    class Website(BASE):
        """
        Website class.
        """
        __tablename__ = "website"
        __table_args__ = {"comment": "Website Table."}

        id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                    comment="ID of the website.")
        base_url = Column(Text, nullable=False, comment="Base URL of website.")
        profile = Column(JSON, nullable=False,
                         comment="Website archiver profile.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")
    BASE.metadata.create_all(bind=ENGINE)
    LOGGER.info("Putting together model")
    MODEL = {
        "website": Website
    }
else:
    LOGGER.info("Putting together model")
    MODEL = {
        table: BASE.classes[classname_for_table(BASE, table, BASE.metadata.tables[table])] for table in
        BASE.metadata.tables
    }

SESSION_FACTORY = sqlalchemy_utility.get_session_factory(ENGINE)
LOGGER.info(f"Model: {MODEL}")


"""
Interfacing functions
"""


def generate_archiving_tables(website_id: str) -> None:
    """
    Function for generating archiving tables.
    :param website_id: Website stem (with underscores instead of dots).
    """
    LOGGER.info(f"Generating archiving tables for website {website_id}")
    website_id = str(website_id)

    class Run(BASE):
        """
        Page dataclass, representing a scraping run of a website.
        """
        __tablename__ = f"{website_id}.runs"
        __table_args__ = {"comment": "Website Run Table."}

        run_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the run.")
        metadata = Column(JSON, nullable=True,
                          comment="Metadata of run.")

        started = Column(DateTime, default=func.now(),
                         comment="Starting timestamp.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        finished = Column(DateTime, nullable=True,
                          comment="Finishing timestamp.")

    class Page(BASE):
        """
        Page dataclass, representing a page of a website.
        """
        __tablename__ = f"{website_id}.pages"
        __table_args__ = {"comment": "Website Page Table."}

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

    class Asset(BASE):
        """
        Page dataclass, representing an asset of a website.
        """
        __tablename__ = f"{website_id}.assets"
        __table_args__ = {"comment": "Website Asset Table."}

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

    class PageLink(BASE):
        """
        Page dataclass, representing the page network of a website.
        """
        __tablename__ = f"{website_id}.page_network"
        __table_args__ = {"comment": "Website Page Network Table."}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{website_id}.pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_page_url = Column(Text, ForeignKey(f"{website_id}.pages.page_url"), nullable=False,
                                 comment="Target page URL of the network link.")

        followed = Column(Boolean, nullable=False, default=False,
                          comment="Flag declaring whether page link was followed.")
        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class ExternalPageLink(BASE):
        """
        Page dataclass, representing the external page network of a website.
        """
        __tablename__ = f"{website_id}.external_page_network"
        __table_args__ = {"comment": "Website External Page Network Table."}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{website_id}.pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_page_url = Column(Text, nullable=False,
                                 comment="Target page URL.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class AssetLink(BASE):
        """
        Page dataclass, representing the asset network of a website.
        """
        __tablename__ = f"{website_id}.asset_network"
        __table_args__ = {"comment": "Website Asset Network Table."}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{website_id}.pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_asset_url = Column(Text, ForeignKey(f"{website_id}.assets.asset_url"), nullable=False,
                                  comment="Target asset URL of the network link.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class Block(BASE):
        """
        Page dataclass, representing a block of a website.
        """
        __tablename__ = f"{website_id}.blocks"
        __table_args__ = {"comment": "Website Block Table."}

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

    class Architecture(BASE):
        """
        Page dataclass, representing an architecture instance of a website.
        """
        __tablename__ = f"{website_id}.architecture"
        __table_args__ = {"comment": "Website Architecture Table."}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of an architecture instance.")
        page_id = Column(Integer, ForeignKey(f"{website_id}.pages.page_id"), nullable=False,
                         comment="Page ID of the architecture instance.")
        block_id = Column(Integer, ForeignKey(f"{website_id}.blocks.block_id"), nullable=False,
                          comment="Block ID of the architecture instance.")
        start_element = Column(Integer, nullable=True,
                               comment="Start element of the block.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class RawPage(BASE):
        """
        Page dataclass, representing a raw page of a website.
        """
        __tablename__ = f"{website_id}.raw_pages"
        __table_args__ = {"comment": "Website Raw Page Table."}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a raw page instance.")
        page_id = Column(Integer, ForeignKey(f"{website_id}.pages.page_id"), nullable=False,
                         comment="Page ID of the instance.")
        raw = Column(Text, nullable=True, comment="Raw content of the page.")
        path = Column(Text, nullable=True,
                      comment="Path to the current offline copy of the page.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class RawAsset(BASE):
        """
        Page dataclass, representing a raw asset of a website.
        """
        __tablename__ = f"{website_id}.raw_assets"
        __table_args__ = {"comment": "Website Raw Asset Table."}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a raw asset instance.")
        asset_id = Column(Integer, ForeignKey(f"{website_id}.assets.asset_id"), nullable=False,
                          comment="Asset ID of the instance.")
        raw = Column(Text, nullable=True, comment="Raw content of the asset.")
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

    for dataclass in [Page, Asset, PageLink, ExternalPageLink, AssetLink, Block, Architecture, RawPage, RawAsset]:
        MODEL[dataclass.__tablename__] = dataclass
    LOGGER.info(f"Model after addition: {MODEL}")
    LOGGER.info("Creating new structures")
    BASE.metadata.create_all(bind=ENGINE)


def add_website_to_archiver(profile: dict) -> Any:
    """
    Function for adding website to archiver.
    :param profile: Website archiver profile.
    :return: Website archiver entry.
    """
    LOGGER.info(f"Adding website with {profile}")
    with SESSION_FACTORY() as session:
        website = MODEL["website"](
            base_url=profile["base_url"],
            profile=profile,
            created=datetime.datetime.now())
        session.add(website)
        session.commit()
        session.refresh(website)

    if website is None:
        return

    generate_archiving_tables(website.id)
    return website


def get_or_create_website_entry(profile: dict) -> Any:
    """
    Function for acquiring or creating website entry.
    :param profile: Archiver profile.
    :return: Website entry.
    """
    LOGGER.info(f"Searching for website entry with {profile}")
    session = SESSION_FACTORY()
    website_entries = session.query(MODEL["website"]).filter(
        MODEL["website"].base_url == profile["base_url"])
    for entry in website_entries:
        if dictionary_utility.check_equality(entry.profile, profile):
            entry.updated = datetime.datetime.now()
            session.commit()
            return entry
    return add_website_to_archiver(profile)


def register_page(website_id: str, page_url: str, page_content: str = None,
                  page_path: str = None) -> None:
    """
    Function for creating or updating links.
    :param website_id: Website ID.
    :param page_url: Page URL.
    :param page_content: Page content. Defaults to None.
    :param page_path: Page path. Defaults to None
    """
    LOGGER.info(f"Registering page for website {website_id}: {page_url}")
    with SESSION_FACTORY() as session:
        page = session.query(MODEL[f"{website_id}.pages"]).filter(
            MODEL[f"{website_id}.pages"].page_url == page_url
        ).first()
        if page is None:
            LOGGER.info(
                f"Found already registered page for website {website_id}: {page_url}")
            page = MODEL[f"{website_id}.pages"](
                page_url=page_url, created=datetime.datetime.now(), inactive="")
            session.add(page)
        elif page.inactive != "":
            page.inactive = ""

        page.updated = datetime.datetime.now()
        session.commit()
        session.refresh(page)

        # Create or update raw page entry, if existing
        if page_content is not None or page_path is not None:
            raw_pages = session.query(MODEL[f"{website_id}.raw_pages"]).filter(
                MODEL[f"{website_id}.raw_pages"].page_id == page.page_id
            ).all()
            for raw_page in raw_pages:
                if raw_page.inactive == "":
                    raw_page.inactive = "x"
                    raw_page.updated = datetime.datetime.now()
            new_raw_page = MODEL[f"{website_id}.raw_pages"](
                page_id=page_url, created=datetime.datetime.now())
            if page_content is not None:
                new_raw_page.raw = page_content
            if page_path is not None:
                new_raw_page.path = page_path
            session.add(new_raw_page)
        session.commit()


def register_asset(website_id: str, source_url: str, asset_url: str, asset_type: str, asset_content: str = None,
                   asset_encoding: str = None, asset_extension: str = None, asset_path: str = None) -> None:
    """
    Function for creating or updating links.
    :param website_id: Website ID.
    :param source_url: Source page URL.
    :param asset_url: Asset URL.
    :param asset_type: Asset type.
    :param asset_content: Asset content. Defaults to None.
    :param asset_encoding: Asset encoding.
    :param asset_extension: Asset extension.
    :param asset_path: Asset path. Defaults to None
    """
    LOGGER.info(f"Registering asset for website {website_id}: {asset_url}")
    with SESSION_FACTORY() as session:
        asset = session.query(MODEL[f"{website_id}.assets"]).filter(
            MODEL[f"{website_id}.assets"].asset_url == asset_url
        ).first()
        if asset is None:
            asset = MODEL[f"{website_id}.assets"](
                asset_url=asset_url, asset_type=asset_type, created=datetime.datetime.now())
            session.add(asset)
        elif asset.inactive != "":
            LOGGER.info(
                f"Found already registered inactivate asset for website {website_id}: {asset_url}")
            asset.inactive = ""
        else:
            LOGGER.info(
                f"Found already registered asset for website {website_id}: {asset_url}")

        asset.updated = datetime.datetime.now()
        session.commit()
        session.refresh(asset)

        # Create or update raw page entry, if existing
        if asset_content is not None or asset_path is not None:
            raw_assets = session.query(MODEL[f"{website_id}.raw_assets"]).filter(
                MODEL[f"{website_id}.raw_assets"].asset_id == asset.asset_id
            ).all()
            for raw_asset in raw_assets:
                if raw_asset.inactive == "":
                    raw_asset.inactive = "x"
                    raw_asset.updated = datetime.datetime.now()
            new_raw_asset = MODEL[f"{website_id}.raw_assets"](
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
            source_page = session.query(MODEL[f"{website_id}.pages"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    MODEL[f"{website_id}.pages"].page_url == source_url)
            ).first()
            if source_page.inactive != "":
                source_page.updated = datetime.datetime.now()
                source_page.inactive = ""

            link = session.query(MODEL[f"{website_id}.asset_network"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    MODEL[f"{website_id}.asset_network"].source_page_url == source_page.page_url,
                    MODEL[f"{website_id}.asset_network"].target_asset_url == asset.asset_url
                )
            ).first()
            if link is None:
                link = MODEL[f"{website_id}.asset_network"](
                    source_page_url=source_page.page_url,
                    target_asset_url=asset.asset_url,
                    created=datetime.datetime.now()
                )
                session.add(link)
            elif link.inactive != "":
                link.inactive = ""
        session.commit()


def register_link(website_id: str, source_url: str, target_url: str, target_type: str) -> bool:
    """
    Function for creating or updating links.
    :param website_id: Website ID.
    :param source_url: Source page URL.
    :param target_url: Target URL.
    :param target_type: Target type: Either 'page' or 'asset'.
    :return: Flag, declaring whether link was already registered.
    """
    LOGGER.info(
        f"Registering link for website {website_id}: {source_url} -> {target_url} ({target_type})")
    target_column = getattr(
        MODEL[f"{website_id}.{target_type}_network"], f"target_{target_type}_url")
    link = None
    with SESSION_FACTORY() as session:
        link = session.query(MODEL[f"{website_id}.{target_type}_network"]).filter(
            sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                MODEL[f"{website_id}.{target_type}_network"].source_page_url == source_url,
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
            session.add(MODEL[f"{website_id}.{target_type}_network"](
                **creation_kwargs
            ))
        else:
            LOGGER.info(
                f"Found already registered link for {source_url} -> {target_url}")
            link.inactive = ""
            link.updated = datetime.datetime.now()
        session.commit()
        return link is None


def get_element_count(website_id: str) -> Tuple[int, int]:
    """
    Function for creating or updating links.
    :param website_id: Website ID.
    :return: Tuple of the numbers of tracked pages and assets.
    """
    LOGGER.info(
        f"Counting {website_id}'s tracked elements...")
    page_count = int(ENGINE.connect().execute(select(func.count()).select_from(
        MODEL[f"{website_id}.pages"])).scalar())
    asset_count = int(ENGINE.connect().execute(select(func.count()).select_from(
        MODEL[f"{website_id}.assets"])).scalar())
    return page_count, asset_count


def get_next_url(website_id: str, page_url: str) -> Optional[str]:
    """
    Function for marking current URL as visited and retrieving next target URL.
    :param website_id: Website ID.
    :param page_url: Current URL.
    :return: Next target URL if found, else None.
    """
    LOGGER.info(f"Finished {website_id}: {page_url}")
    next_link = None
    with SESSION_FACTORY() as session:
        followed = session.query(MODEL[f"{website_id}.page_network"]).filter(
            sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                MODEL[f"{website_id}.page_network"].followed == False,
                MODEL[f"{website_id}.page_network"].target_page_url == page_url)
        ).all()
        for entry in followed:
            entry.followed = True
            entry.updated = datetime.datetime.now()
        session.commit()
        LOGGER.info(f"Updated {website_id}: {page_url} links")

        while next_link is None:
            next_link = session.query(MODEL[f"{website_id}.page_network"]).filter(
                MODEL[f"{website_id}.page_network"].followed == False
            ).first()
            if next_link is None:
                break

            alredy_visited = session.query(MODEL[f"{website_id}.page_network"]).filter(
                sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                    MODEL[f"{website_id}.page_network"].followed == True,
                    MODEL[f"{website_id}.page_network"].target_page_url == next_link.target_page_url)
            ).first()
            if alredy_visited is not None:
                for alredy_visited in session.query(MODEL[f"{website_id}.page_network"]).filter(
                    sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                        MODEL[f"{website_id}.page_network"].followed == False,
                        MODEL[f"{website_id}.page_network"].target_page_url == next_link.target_page_url)
                ).all():
                    alredy_visited.followed = True
                    alredy_visited.updated = datetime.datetime.now()
                session.commit()
                next_link = None
            else:
                next_link = next_link.target_page_url
    return next_link


def check_for_existence(website_id: str, url: str, target_type: str) -> bool:
    """
    Function for marking current URL as visited and retrieving next target URL.
    :param website_id: Website ID.
    :param url: Target URL.
    :param target_type: Target type: Either 'page' or 'asset'.
    :return: Flag, declaring whether target was already registered.
    """
    LOGGER.info(f"Checking for existence {website_id}: {url} ({target_type})")
    found = False
    url_column = getattr(
        MODEL[f"{website_id}.{target_type}s"], f"{target_type}_url")
    inactive_column = getattr(
        MODEL[f"{website_id}.{target_type}s"], f"inactive")
    with SESSION_FACTORY() as session:
        entry = session.query(MODEL[f"{website_id}.page_network"]).filter(
            sqlalchemy_utility.SQLALCHEMY_FILTER_CONVERTER["&&"](
                url_column == False,
                inactive_column == "")
        ).first()
        found = entry is not None
    return found
