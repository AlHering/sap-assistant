# -*- coding: utf-8 -*-
"""
****************************************************
*                ScrapingService                 
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
from time import sleep
import pandas
import logging
from lxml import html
import traceback
import requests
from src.configuration import configuration as cfg
from src.utility.bronze import json_utility


class MediaMetadata(object):
    """
    MediaMetadata class, managing asset metadata functionality around different media types.
    """

    def __init__(self) -> None:
        """
        Initiation method for MediaMetadata objects.
        """
        self._logger = logging.Logger("[MediaMetadata]")
        self._logger.info(f"Initiating MediaMetadata {self}")
        self.media_path = f"{cfg.PATHS.DATA_PATH}/processes/media_types"
        self.media = json_utility.load(
            os.path.join(self.media_path, "media_types.json"))

    def load_metadata_from_disk(self) -> None:
        """
        Method for loading metadata base from disk.
        """
        self._logger.info("Loading metadata base from disk ...")
        no_name = []
        for file in ["application.csv", "font.csv", "model.csv", "text.csv", "audio.csv", "image.csv",
                     "message.csv", "multipart.csv", "video.csv"]:
            self._logger.info(f"Loading metadata base from '{file}' ...")
            df = pandas.read_csv(os.path.join(self.media_path, file))
            topic = file.replace(".csv", "")
            if topic not in self.media:
                self.media[topic] = {}
            for index, row in df.iterrows():
                self._logger.info(f"Checking entry {row} ...")
                i = 0
                while i < 3:
                    try:
                        if "Name" not in row:
                            no_name.append((topic, index))
                        elif row["Name"] not in self.media[topic]:
                            self.media[topic][row["Name"]] = {
                                "template": row["Template"],
                                "reference": row["Reference"],
                                "description": requests.get(
                                    f"https://www.iana.org/assignments/media-types/{row['Template']}").text if row[
                                    'Template'] != "" else ""
                            }
                        elif self.media[topic][row["Name"]]["template"] and not \
                                self.media[topic][row["Name"]]["description"]:
                            self.media[topic][row["Name"]]["description"] = requests.get(
                                f"https://www.iana.org/assignments/media-types/{row['Template']}").text
                        i = 3
                    except Exception as ex:
                        self._logger.warning(f"Exception appeared: {ex}:")
                        self._logger.warning(traceback.format_exc())
                        i += 1
                        sleep(3)
                self._logger.info(
                    f"{self.media[file.replace('.csv', '')][row['Name']]} was retrieved.")
            json_utility.save(
                self.media, os.path.join(self.media_path, "media_types.json"))
        if no_name:
            self._logger.warning(f"No name found for {len(no_name)} entries:")
            self._logger.warning(str(no_name))

    def enrich_base_data(self) -> None:
        """
        Method for enriching base data.
        """
        self._logger.info("Enriching metadata with extensions and details ...")
        self._enrich_from_freeformatter()
        self._enrich_from_resplace()
        self._logger.info("Finished enrichment process.")

    def _enrich_from_freeformatter(self) -> None:
        """
        Internal method for enriching from metadata from source www.freeformatter.com.
        """
        self._logger.info("Collecting metadata from www.freeformatter.com ...")
        page = requests.get(
            "https://www.freeformatter.com/mime-types-list.html").content.decode("utf-8")
        open(f"{cfg.PATHS.DATA_PATH}/media_types/mime-types-list.html",
             "w", encoding="utf-8").write(page)
        html_tree = html.fromstring(page)
        table = html_tree.xpath(
            "//table[@class='table table-striped table-sort']")[0]
        table_data = [[elem.xpath("./text()")[0]
                       for elem in table.xpath("./thead/tr/th")]]

        for row in table.xpath("./tbody/tr"):
            row_data = [elem.xpath("./text()")[0].lower()
                        if elem.xpath("./text()") else "" for elem in row]
            row_data[-1] = row.xpath("./td/a/@href")[0].lower()
            table_data.append(row_data)

        for entry in table_data[1:]:
            topic, name = entry[1].split("/")
            if topic in self.media:
                if name in self.media[topic]:
                    self.media[topic][name]["name"] = entry[1]
                    self.media[topic][name]["extension"] = entry[2] if entry[2] != "n/a" else None
                    self.media[topic][name]["details"] = entry[3]
                else:
                    self.media[topic][name] = {
                        "name": entry[1],
                        "extension": entry[2] if entry[2] != "n/a" else None,
                        "details": entry[3]
                    }
            else:
                self.media[topic] = {
                    name: {
                        "name": entry[1],
                        "extension": entry[2] if entry[2] != "n/a" else None,
                        "details": entry[3]
                    }
                }
        json_utility.save(
            self.media, os.path.join(self.media_path, "media_types.json"))
        self._logger.info(
            "Finished metadata collection from www.freeformatter.com ...")

    def _enrich_from_resplace(self) -> None:
        """
        Internal method for enriching from metadata from source www.resplace.com.
        """
        self._logger.info(
            "Filling gaps with metadata from www.resplace.com  ...")
        page = requests.get(
            "https://resplace.com/online-tools/developer/mime-type-database").content.decode("utf-8")
        open(f"{cfg.PATHS.DATA_PATH}/media_types/mime-type-database.html",
             "w", encoding="utf-8").write(page)
        html_tree = html.fromstring(page)
        cards = html_tree.xpath("//div[@class='alert alert-secondary ']")
        for card in cards:
            extension = card.xpath("./span/text()")
            if extension:
                topic, name = card.xpath("./b/text()")[0].split("/")
                if topic in self.media:
                    if name not in self.media[topic]:
                        self.media[topic][name] = {
                            "extension": "." + extension[0].lower()}
                    elif "extension" not in self.media[topic][name]:
                        self.media[topic][name]["extension"] = "." + \
                            extension[0].lower()
                else:
                    self.media[topic] = {
                        name: {"extension": "." + extension[0].lower()}
                    }
        json_utility.save(
            self.media, os.path.join(self.media_path, "media_types.json"))
        self._logger.info(
            "Finished metadata collection from www.resplace.com ...")

    def accumulate(self, field: str, excluded_types: list = []) -> set:
        """
        Method for accumulating a target field across all types except the ones, noted in excluded_types.
        :param field: Target field.
        :param excluded_types: Excluded main types.
        :return: Set of accumulated values-
        """
        acc = []
        for main_type in [main_type for main_type in self.media if main_type not in excluded_types]:
            for sub_type in self.media[main_type]:
                acc.append(self.media[main_type][sub_type].get(field))
        return set(acc)
