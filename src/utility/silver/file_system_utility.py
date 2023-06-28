# -*- coding: utf-8 -*-
"""
****************************************************
*                     Utility                      *
*            (c) 2022 Alexander Hering             *
****************************************************
"""
import os
from typing import List


def create_folder_tree(root: str, structure: list) -> None:
    """
    Function for creating folder tree.
    :param root: Root path of the folder tree.
    :param structure: List with potentially nested folder names.
    """
    safely_create_path(root)
    for elem in structure:
        if isinstance(elem, list):
            create_folder_tree(os.path.join(root, elem[0]), elem[1:])
        else:
            safely_create_path(os.path.join(root, elem))


def clean_path(path: str) -> str:
    """
    Function for cleaning paths by replacing double backslashes with single forward slashes.
    :param path: Path to be cleaned.
    :return: Cleaned path.
    """
    return path.replace("\\", "/")


def get_folder_content(folder: str, content_type: str) -> List[str]:
    """
    Function for getting content of folder by content type.
    :param folder: Folder to get content from.
    :param content_type: Content type as string ('directory'/'folder', 'file').
    :return: List of content elements.
    """
    for root, dirs, files in os.walk(folder, topdown=True):
        if content_type.startswith("director") or content_type.startswith("folder"):
            return dirs
        elif content_type.startswith("file"):
            return files
        break


def safely_create_path(path: str) -> None:
    """
    Function for safely creating folder path.
    :param path: Folder path to create.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_all_files(path: str) -> List[str]:
    """
    Function for collecting all files (including nested files) under given directory.
    :param path: Root path to start file search in.
    """
    file_list = []
    for root, dirs, files in os.walk(path, topdown=True):
        for file in files:
            file_list.append(os.path.join(root, file))
    return list(set(file_list))


def get_all_folders(path: str) -> List[str]:
    """
    Function for collecting all folders (including nested folders) under given directory.
    :param path: Root path to start folder search in.
    """
    folder_list = []
    for root, dirs, files in os.walk(path, topdown=True):
        for folder in dirs:
            folder_list.append(os.path.join(root, folder))
    return list(set(folder_list))
