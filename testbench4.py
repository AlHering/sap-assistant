# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
from typing import List
from multiprocessing import Process
from src.configuration import configuration as cfg
from src.model.scraping_control.archiving.requests_website_archiver import RequestsWebsiteArchiver
from src.utility.bronze import json_utility, sqlalchemy_utility
from src.utility.silver import file_system_utility


def print_fk_info(tables: List[sqlalchemy_utility.Table]) -> None:
    """
    Function for printing foreign key information for tables.
    :param tables: List of table objects.
    """
    for table in tables:
        print(f"\n\n{table}")
        for column in tables[table].columns:
            print(f"Column: {column}")
        for foreign_key in tables[table].foreign_keys:
            print(f"FK: {foreign_key}")
            print(f"FK column: {foreign_key.column}")
            print(f"FK target fullname: {foreign_key.target_fullname}")


def run_migration(source_db_uri: str, target_db_uri: str, source_tables: List[str], target_tables: List[str]) -> None:
    """
    Function for running migration.
    :param source_db_uri: Source database URI.
    :param target_db_uri: Target database URI.
    :param source_tables: Source tables.
    :param target_tables: Target tables.
    """
    print("Running migration")
    sqlalchemy_utility.migrate(
        source_db_uri, target_db_uri, source_tables, target_tables)


def run_parallel_migration(profiles: List[str]) -> None:
    """
    Function for running multiple migrations in parallel.
    :param profiles: List of profile names.
    """
    processes = []
    for profile_name in profiles:
        profile = json_utility.load(
            f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
        source_db_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/backups/{profile_name}.db"
        target_db_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
        profile["database_uri"] = source_db_uri
        archiver = RequestsWebsiteArchiver(profile)
        db = archiver.database
        tables = db.base.metadata.tables
        source_tables = [t for t in tables if t.startswith("1.")]
        target_tables = [t for t in tables if t.startswith(
            file_system_utility.clean_directory_name(profile["base_url"]))]
        processes.append(Process(target=run_migration,
                         args=(source_db_uri, target_db_uri, source_tables, target_tables,)))
        processes[-1].start()
    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        for process in processes:
            process.kill()


def migrate_runs_to_db(profiles: List[str]) -> None:
    """
    Function for migrating runs to DB.
    :param profiles: List of profile names.
    """
    for profile_name in profiles:
        profile = json_utility.load(
            f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
        target_db_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
        profile["database_uri"] = target_db_uri
        dump_folder = profile.get("dump_path", os.path.join(
            cfg.PATHS.DUMP_PATH, "website_archiver", file_system_utility.clean_directory_name(profile["base_url"])))
        dumped_caches = []
        archiver = RequestsWebsiteArchiver(profile, reload_last_state=False)

        found_finished = False
        for root, _, files in os.walk(dump_folder, topdown=True):
            for f in files:
                if "_FINISHED" in f:
                    dumped_caches = [os.path.join(root, f)]
                    found_finished = True
            if not found_finished:
                dumped_caches = [
                    os.path.join(root, file) for file in files if file.startswith("MILESTONE")]
            break
        if dumped_caches:
            dump = json_utility.load(dumped_caches[-1])
            cache = {
                key: dump["_cache"][key] for key in dump["_cache"]
            }
            for key in ["failed", "reason"]:
                cache[key] = dump[key]
            archiver.cache = cache
            archiver.save_state(["session"], finished=isinstance(
                dump["reason"], str) and dump["reason"] == "archiving_finished")


if __name__ == "__main__":
    profiles = ["tcodesearch_com", "sapdatasheet_org",
                "sap4tech_net", "erpgreat_com", "erp-up_de"]
    # profiles = ["se80_co_uk"]
    migrate_runs_to_db(profiles)
