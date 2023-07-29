# -*- coding: utf-8 -*-
"""
****************************************************
*           aura-cognitive-architecture
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from . import urls as URLS
from . import paths as PATHS
from dotenv import dotenv_values
import os
import logging


"""
Environment file
"""
ENV = dotenv_values(os.path.join(PATHS.PACKAGE_PATH, ".env"))

"""
Logger
"""


class LOGGER_REPLACEMENT(object):
    """
    Temporary logger replacement class.
    """

    def debug(self, text: str) -> None:
        """
        Method replacement for logging.
        :param text: Text to log.
        """
        print(f"[DEBUG] {text}")

    def info(self, text: str) -> None:
        """
        Method replacement for logging.
        :param text: Text to log.
        """
        print(f"[INFO] {text}")

    def warning(self, text: str) -> None:
        """
        Method replacement for logging.
        :param text: Text to log.
        """
        print(f"[WARNING] {text}")


# LOGGER = logging.Logger("SAPAssistant")
# LOGGER.setLevel(level=logging.INFO)
LOGGER = LOGGER_REPLACEMENT()
