# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from src.model.scraping_control.archiving.requests_website_archiver import RequestsWebsiteArchiver
import os
from multiprocessing import Process
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility
from src.model.scraping_control.archiving.parameterized_website_database import WebsiteDatabase
from src.model.scraping_control.archiving import website_archiver
website_archiver.website_database


def run_archiver(profile: dict) -> None:
    """
    Function for running profile-based archiver.
    :param profile: Archiver profile.
    """

    archiver = RequestsWebsiteArchiver(profile)
    archiver.archive_website()


if __name__ == "__main__":
    processes = []
    for profile_name in []:
        profile = json_utility.load(
            f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
        database_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
        profile["database_uri"] = database_uri
        processes.append(Process(target=run_archiver, args=(profile,)))
        processes[-1].start()
    for process in processes:
        process.join()
