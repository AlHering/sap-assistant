# -*- coding: utf-8 -*-
"""
****************************************************
*                     Utility                      *
*            (c) 2023 Alexander Hering             *
****************************************************
"""
from typing import Any
from time import sleep
from datetime import datetime as dt


def wait_for_state_variable(streamlit_context: Any, variable_name: str, waiting_message: str, timeout: float = -1.0) -> None:
    """
    Function for waiting for state variable to be available.
    :param streamlit_context: Streamlit context.
    :param variable_name: Variable name of state variable to wait for.
    :param waiting_message: Waiting message to display with a spinner while waiting.
    :param timeout: Time to wait in seconds before raising a timeout error.
        Defaults to -1 in which case no timeout error is raised.
    :raises: TimeoutError if timeout is larger than -1 and is exceeded.
    """
    with streamlit_context.spinner(waiting_message):
        start = dt.now()
        while variable_name not in streamlit_context.session_state:
            sleep(0.1)
            if timeout >= 0 and dt.now() - start >= timeout:
                raise TimeoutError(
                    f"Timeout while waiting for '{variable_name}' to be available under Streamlit context.")
