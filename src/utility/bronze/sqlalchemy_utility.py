# -*- coding: utf-8 -*-
"""
****************************************************
*                     Utility
*            (c) 2022 Alexander Hering             *
****************************************************
"""
import copy
from sqlalchemy import Column, String, Boolean, Integer, JSON, Text, DateTime, CHAR, ForeignKey, Table, Float, BLOB, TEXT
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_, or_, not_
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base, classname_for_table
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy import orm, inspect
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import ProgrammingError, OperationalError
from typing import List, Union, Any, Optional


# Supported dialects
SUPPORTED_DIALECTS = ["sqlite", "mysql",
                      "mssql", "postgresql", "mariadb", "oracle"]
# Conversion dictionary for SQLAlchemy typing
SQLALCHEMY_TYPING_DICTIONARY = {
    "int": Integer,
    "dict": JSON,
    "datetime": DateTime,
    "str": String(60),
    "str_": String,
    "text": Text,
    "bool": Boolean,
    "char": CHAR,
    "longtext": Text,
    "float_": Float,
    "float": Float,
}


def get_engine(engine_url: str, pool_recycle: int = 280, encoding: str = "utf-8") -> Engine:
    """
    Function for getting database engine.
    :param engine_url: URL to create engine for.
    :param pool_recycle: Parameter for preventing the reuse of connections that were stale for some time.
    :param encoding: Encoding string. Defaults to 'utf-8'.
    :return: Engine to given database.
    """
    try:
        # SQLAlchemy 1.4
        return create_engine(engine_url, encoding=encoding, pool_recycle=pool_recycle)
    except TypeError:
        # SQLAlchemy 2.0
        return create_engine(engine_url, pool_recycle=pool_recycle)


def execute_command(engine: Engine, command: str) -> Optional[Any]:
    """
    Function for executing commands via database engine.
    :param engine: Database engine.
    :param command: Command to execute.
    :return: Return value of command, if existing.
    """
    return engine.execute(text(command))


def get_session_factory(engine: Engine) -> Any:
    """
    Function for getting database session factory.
    :param engine: Engine to bind session factory to.
    :return: Engine to given database.
    """
    return orm.scoped_session(
        orm.sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine,
        ),
    )


def get_automapped_base(engine: Engine) -> Any:
    """
    Function for getting prepared automap base.
    :param engine: Engine to bind session factory to.
    :return: Automap base.
    """
    base = automap_base()
    base.prepare(autoload_with=engine, reflect=True)
    return base


def get_classes_from_base(base: Any) -> dict:
    """
    Function for getting class dictionary for existing tables.
    :param base: Base to get classes from.
    :return: Class dictionary, mapping entity name to ORM class.
    """
    return {table: base.classes[classname_for_table(base, table, base.metadata.tables[table])] for table in
            base.metadata.tables}


def create_mapping_from_dictionary(mapping_base: Any, entity_type: str, column_data: dict, linkage_data: dict = None, typing_translation: dict = SQLALCHEMY_TYPING_DICTIONARY) -> Any:
    """
    Function for creating database mapping from dictionary.
    :param mapping_base: Mapping base class.
    :param entity_type: Entity type to create mapping for.
    :param column_data: Column data dictionary.
    :param linkage_data: Linkage data dictionary. Defaults to None
    :param typing_translation: Typing translation dictionary. Defaults to default sqlalchemy-translation.
    :return: Mapping class.
    """
    class_data = {"__tablename__": entity_type}
    desc = column_data.get("#meta", {}).get("description", False)
    if column_data.get("#meta", False):
        class_data["__table_args__"] = copy.deepcopy(column_data["#meta"])

    class_data.update(
        {
            param: Column(typing_translation[column_data[param]["type"]], **column_data[param].get("schema_args", {})) if "_" not in column_data[param]["type"]
            else Column(typing_translation[column_data[param]["type"].split("_")[0] + "_"](*[int(arg) for arg in column_data[param]["type"].split("_")[1:]]), **column_data[param].get("schema_args", {}))
            for param in column_data if param != "#meta"
        }
    )
    if linkage_data is not None:
        for profile in [profile for profile in linkage_data if
                        linkage_data[profile]["linkage_type"] == "foreign_key" and linkage_data[profile][
                            "source"] == entity_type]:
            target_class = linkage_data[profile]["target"][0].upper(
            ) + linkage_data[profile]["target"][1:]
            if linkage_data[profile]["relation"] == "1:1":
                class_data.update({
                    profile: relationship(target_class, back_populates=profile, uselist=False)})
            elif linkage_data[profile]["relation"] == "1:n":
                class_data.update({profile: relationship(
                    target_class, back_populates=profile)})
            elif linkage_data[profile]["relation"] == "n:m":
                class_data.update({profile: relationship(target_class, secondary=Table(
                    profile,
                    mapping_base.metadata,
                    Column(f"{entity_type}_{linkage_data[profile]['source_key'][1]}",
                           ForeignKey(f"{entity_type}.{linkage_data[profile]['source_key'][1]}")),
                    Column(f"{linkage_data[profile]['target']}_{linkage_data[profile]['target_key'][1]}",
                           ForeignKey(f"{entity_type}.{linkage_data[profile]['source_key'][1]}"))
                ))})
        for profile in [profile for profile in linkage_data if
                        linkage_data[profile]["linkage_type"] == "foreign_key" and linkage_data[profile][
                            "target"] == entity_type]:
            source_class = linkage_data[profile]["source"][0].upper(
            ) + linkage_data[profile]["source"][1:]
            source = linkage_data[profile]["source"]
            source_key = linkage_data[profile]["source_key"][1]
            if linkage_data[profile]["relation"].startswith("1:"):
                class_data.update({
                    f"{source}_{source_key}": Column(
                        typing_translation[linkage_data[profile]
                                           ["source_key"][0]],
                        ForeignKey(f"{source}.{source_key}")
                    ),
                    profile: relationship(source_class, back_populates=profile)
                })
    return type(entity_type[0].upper()+entity_type[1:], (mapping_base,), class_data)
