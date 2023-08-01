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
from src.utility.bronze import json_utility


def run_archiver(profile: dict, wait: float = .0) -> None:
    """
    Function for running profile-based archiver.
    :param profile: Archiver profile.
    :param wait: Time to wait in seconds before starting.
    """
    sleep(wait)
    archiver = RequestsWebsiteArchiver(profile)
    archiver.archive_website()


if __name__ == "__main__":
    processes = []
    counter = .0
    for profile_name in ["se80_co_uk", "tcodesearch_com", "sapdatasheet_org", "sap4tech_net", "erpgreat_com", "erp-up_de"]:
        profile = json_utility.load(
            f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
        database_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
        profile["database_uri"] = database_uri
        processes.append(Process(target=run_archiver,
                         args=(profile, counter*10)))
        processes[-1].start()
        counter += 1
    for process in processes:
        process.join()
