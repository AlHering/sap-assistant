# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility
from src.model.scraping_control.archiving.requests_website_database_archiver import RequestsWebsiteArchiver

if __name__ == "__main__":
    profile = json_utility.load(
        f"{cfg.PATHS.DATA_PATH}/processes/profiles/se80_co_uk.json")
    database_uri = f"sqlite:///{cfg.PATHS.DATA_PATH}/processes/se80_co_uk.db"
    profile["database_uri"] = database_uri
    archiver = RequestsWebsiteArchiver(profile)
    archiver.archive_website()
