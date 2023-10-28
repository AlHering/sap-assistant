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


if __name__ == "__main__":
    profile_name = "se80_co_uk"

    profile = json_utility.load(
        f"{cfg.PATHS.DATA_PATH}/processes/profiles/{profile_name}.json")
    database_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/{profile_name}.db"
    profile["database_uri"] = database_uri
    archiver = RequestsWebsiteArchiver(profile)
    db = archiver.database
    print(sqlalchemy_utility.get_classes_from_base(db.base))
