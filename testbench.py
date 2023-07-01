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
from src.model.scraping_control.archiving.requests_website_archiver_v2 import RequestsWebsiteArchiver

if __name__ == "__main__":

    PROFILE_PATH = ""
    archiver = RequestsWebsiteArchiver(
        json_utility.load(PROFILE_PATH))
    # archiver.load_state_dump(os.path.join(
    #    archiver.profile["offline_copy_path"], "milestone.json"))
    archiver.archive_website()
