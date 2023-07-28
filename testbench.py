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
from src.model.scraping_control.archiving.requests_website_archiver import RequestsWebsiteArchiver

if __name__ == "__main__":
    PROFILE_PATH = f"{cfg.PATHS.DATA_PATH}/processes/profiles/se80_co_uk.json"
    archiver = RequestsWebsiteArchiver(
        json_utility.load(PROFILE_PATH))
    archiver.archive_website()
