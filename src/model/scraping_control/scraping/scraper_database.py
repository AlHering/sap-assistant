# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from sqlalchemy import Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, func, select
from sqlalchemy.ext.automap import automap_base
from typing import Any, List, Tuple, Optional
import datetime
from src.configuration import configuration as cfg
from src.utility.bronze import sqlalchemy_utility


class SAPObjectDatabase(object):
    """
    Class, representing an SAP object database.
    """

    def __init__(self, database_uri: str = None, schema: str = "", verbose: bool = False) -> None:
        """
        Initiation method.
        :param database_uri: Database URI.
            Defaults to None in which case the central SAP_DB ENV variable is used.
        :param schema: Schema to use.
            Defaults to empty string in which case no schema is used.
        :param verbose: Verbose flag for interaction methods.
            Defaults to False for the case the scrapers are already logging.
        """
        self._logger = cfg.LOGGER
        self.verbose = verbose
        self._logger.info("Automapping existing structures")
        self.base = automap_base()
        self.engine = sqlalchemy_utility.get_engine(
            cfg.ENV["SAP_DB"] if database_uri is None else database_uri)
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

        class Table(self.base):
            """
            Page dataclass, representing a table.
            """
            __tablename__ = f"{self.schema}tables"
            __table_args__ = {
                "comment": "SAP table table.", "extend_existing": True}

            id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the entry.")
            url = Column(Text, nullable=False, unique=True,
                         comment="URL of entry.")

            description = Column(Text,
                                 comment="Description of entry.")
            meta_data = Column(JSON,
                               comment="Metadata of entry.")
            keys_fields = Column(JSON,
                                 comment="Key fields of entry.")
            table_fields = Column(JSON,
                                  comment="Table fields of entry.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class Structure(self.base):
            """
            Page dataclass, representing a structure.
            """
            __tablename__ = f"{self.schema}structures"
            __table_args__ = {
                "comment": "SAP structure table.", "extend_existing": True}

            id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the entry.")
            url = Column(Text, nullable=False, unique=True,
                         comment="URL of entry.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class Transaction(self.base):
            """
            Page dataclass, representing a transaction.
            """
            __tablename__ = f"{self.schema}transactions"
            __table_args__ = {
                "comment": "SAP transaction table.", "extend_existing": True}

            id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the entry.")
            url = Column(Text, nullable=False, unique=True,
                         comment="URL of entry.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class ABAPClasses(self.base):
            """
            Page dataclass, representing a transaction.
            """
            __tablename__ = f"{self.schema}abap_classes"
            __table_args__ = {
                "comment": "SAP ABAP class table.", "extend_existing": True}

            id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the entry.")
            url = Column(Text, nullable=False, unique=True,
                         comment="URL of entry.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        class ABAPMethods(self.base):
            """
            Page dataclass, representing an ABAP method.
            """
            __tablename__ = f"{self.schema}abap_method"
            __table_args__ = {
                "comment": "SAP ABAP method table.", "extend_existing": True}

            id = Column(Integer, primary_key=True, autoincrement=True, unique=True, nullable=False,
                        comment="ID of the entry.")
            url = Column(Text, nullable=False, unique=True,
                         comment="URL of entry.")

            created = Column(DateTime, default=func.now(),
                             comment="Timestamp of creation.")
            updated = Column(DateTime, onupdate=func.now(),
                             comment="Timestamp of last update.")
            inactive = Column(CHAR, default="",
                              comment="Flag for marking inactive entries.")

        for dataclass in [Table, Structure, Transaction, ABAPClasses, ABAPMethods]:
            self.model[dataclass.__tablename__] = dataclass
        if self.verbose:
            self._logger.info(f"self.model after addition: {self.model}")
        self._logger.info("Creating new structures")
        self.base.metadata.create_all(bind=self.engine)

    """
    Interfacing methods
    """
