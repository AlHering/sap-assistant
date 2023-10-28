# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from sqlalchemy.orm import relationship, mapped_column, declarative_base
from sqlalchemy import Engine, Column, String, JSON, ForeignKey, Integer, DateTime, func, Uuid, Text, event, Boolean, CHAR
from uuid import uuid4, UUID
from typing import Any


def populate_data_instrastructure(engine: Engine, schema: str, model: dict) -> None:
    """
    Function for populating data infrastructure.
    :param engine: Database engine.
    :param schema: Schema for tables.
    :param model: Model dictionary for holding data classes.
    """
    schema = str(schema)
    if not schema.endswith("."):
        schema += "."
    base = declarative_base()

    class Run(base):
        """
        Page dataclass, representing a scraping run of a website.
        """
        __tablename__ = f"{schema}runs"
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

    class Page(base):
        """
        Page dataclass, representing a page of a website.
        """
        __tablename__ = f"{schema}pages"
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

    class Asset(base):
        """
        Page dataclass, representing an asset of a website.
        """
        __tablename__ = f"{schema}assets"
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

    class PageLink(base):
        """
        Page dataclass, representing the page network of a website.
        """
        __tablename__ = f"{schema}page_network"
        __table_args__ = {
            "comment": "Website Page Network Table.", "extend_existing": True}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{schema}pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_page_url = Column(Text, ForeignKey(f"{schema}pages.page_url"), nullable=False,
                                 comment="Target page URL of the network link.")

        followed = Column(Boolean, nullable=False, default=False,
                          comment="Flag declaring whether page link was followed.")
        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class ExternalPageLink(base):
        """
        Page dataclass, representing the external page network of a website.
        """
        __tablename__ = f"{schema}external_page_network"
        __table_args__ = {
            "comment": "Website External Page Network Table.", "extend_existing": True}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{schema}pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_page_url = Column(Text, nullable=False,
                                 comment="Target page URL.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class AssetLink(base):
        """
        Page dataclass, representing the asset network of a website.
        """
        __tablename__ = f"{schema}asset_network"
        __table_args__ = {
            "comment": "Website Asset Network Table.", "extend_existing": True}

        link_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                         comment="ID of a network link.")
        source_page_url = Column(Text, ForeignKey(f"{schema}pages.page_url"), nullable=False,
                                 comment="Source page URL of the network link.")
        target_asset_url = Column(Text, ForeignKey(f"{schema}assets.asset_url"), nullable=False,
                                  comment="Target asset URL of the network link.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class Block(base):
        """
        Page dataclass, representing a block of a website.
        """
        __tablename__ = f"{schema}blocks"
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

    class Architecture(base):
        """
        Page dataclass, representing an architecture instance of a website.
        """
        __tablename__ = f"{schema}architecture"
        __table_args__ = {
            "comment": "Website Architecture Table.", "extend_existing": True}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of an architecture instance.")
        page_id = Column(Integer, ForeignKey(f"{schema}pages.page_id"), nullable=False,
                         comment="Page ID of the architecture instance.")
        block_id = Column(Integer, ForeignKey(f"{schema}blocks.block_id"), nullable=False,
                          comment="Block ID of the architecture instance.")
        start_element = Column(Integer, nullable=True,
                               comment="Start element of the block.")

        created = Column(DateTime, default=func.now(),
                         comment="Timestamp of creation.")
        updated = Column(DateTime, onupdate=func.now(),
                         comment="Timestamp of last update.")
        inactive = Column(CHAR, default="",
                          comment="Flag for marking inactive entries.")

    class RawPage(base):
        """
        Page dataclass, representing a raw page of a website.
        """
        __tablename__ = f"{schema}raw_pages"
        __table_args__ = {
            "comment": "Website Raw Page Table.", "extend_existing": True}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a raw page instance.")
        page_id = Column(Integer, ForeignKey(f"{schema}pages.page_id"), nullable=False,
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

    class RawAsset(base):
        """
        Page dataclass, representing a raw asset of a website.
        """
        __tablename__ = f"{schema}raw_assets"
        __table_args__ = {
            "comment": "Website Raw Asset Table.", "extend_existing": True}

        instance_id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                             comment="ID of a raw asset instance.")
        asset_id = Column(Integer, ForeignKey(f"{schema}assets.asset_id"), nullable=False,
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
        model[dataclass.__tablename__] = dataclass

    base.metadata.create_all(bind=engine)
