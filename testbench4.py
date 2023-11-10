# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from time import sleep
from multiprocessing import Process
from src.configuration import configuration as cfg
from src.model.scraping_control.archiving.requests_website_archiver import RequestsWebsiteArchiver
from src.utility.bronze import json_utility, sqlalchemy_utility


def print_fk_info(tables):
    for table in tables:
        print(f"\n\n{table}")
        for column in tables[table].columns:
            print(f"Column: {column}")
        for foreign_key in tables[table].foreign_keys:
            print(f"FK: {foreign_key}")
            print(f"FK column: {foreign_key.column}")
            print(f"FK target fullname: {foreign_key.target_fullname}")


def run_migration(database_uri, st, tt):
    sqlalchemy_utility.migrate(database_uri, database_uri, st, tt)


if __name__ == "__main__":
    profile_name = "se80_co_uk"

    profile = json_utility.load(
        f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
    database_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
    profile["database_uri"] = database_uri
    archiver = RequestsWebsiteArchiver(profile)
    db = archiver.database
    source_classes = sqlalchemy_utility.get_classes_from_base(db.base)
    tables = db.base.metadata.tables
    st = [t for t in tables if t.startswith("1.")]
    tt = [t for t in tables if t.startswith("https-__www-se80-co-uk_.")]
    run_migration(database_uri, st, tt)
