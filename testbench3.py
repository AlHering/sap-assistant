# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import requests
from lxml import html
from src.model.scraping_control.scraping.scraping_module import TableScrapingModule


def testing_callback(target_type: str, entry_data: dict) -> None:
    """
    Function for printing out scraping results.
    :param target_type: Target type.
    :param entry_data: Collected entry data.
    """
    print("="*20)
    print(f"Entry type: {target_type}")
    for key in entry_data:
        print(f"{key}: {entry_data[key]}")
    print("="*20)


if __name__ == "__main__":
    module = TableScrapingModule(
        ["https://www.se80.co.uk/"], "table", testing_callback)

    response = requests.get(
        "https://www.se80.co.uk/sap-tables/?name=ekko")
    print(response.content)
    html_content = html.fromstring(
        response.content if response.content else "<!DOCTYPE html><html>")
    print(module.active(response.url, html_content))
    if module.active(response.url, html_content):
        module.scrape(response.url, html_content)
