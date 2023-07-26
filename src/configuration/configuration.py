# -*- coding: utf-8 -*-
"""
****************************************************
*           aura-cognitive-architecture
*            (c) 2023 Alexander Hering             *
****************************************************
"""
import os
import logging
from dotenv import dotenv_values
from . import paths as PATHS
from . import urls as URLS


"""
Environment file
"""
ENV = dotenv_values(os.path.join(PATHS.PACKAGE_PATH, ".env"))

"""
Logger
"""
LOGGER = logging.Logger("SAPAssistant")
