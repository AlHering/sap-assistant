
from typing import List, Any
from src.configuration import configuration as cfg
from src.model.scraping_control.website_archiver_profiles import ENTITY_PROFILE, LINKAGE_PROFILE, VIEW_PROFILE
from src.utility.gold.sqlalchemy_entity_data_interface import SQLAlchemyEntityInterface as DBInterface
from src.utility.gold.filter_mask import FilterMask


class ArchiverDatabase(DBInterface):
    """
    Class, representing website archiver database interfaces.
    """

    def __init__(self) -> None:
        """
        Initiation method.
        """
        super().__init__(environment_profile={
            "backend": "database",
            "framework": "sqlalchemy",
            "arguments": {
                "database": cfg.ENV["WEBSITE_ARCHIVER_DB_URL"],
                "dialect": cfg.ENV["WEBSITE_ARCHIVER_DB_DIALECT"],
                "encoding": "utf-8"
            },
            "targets": "*",
            "handle_as_objects": True
        },
            entity_profiles=ENTITY_PROFILE,
            linkage_profiles=LINKAGE_PROFILE,
            view_profiles=VIEW_PROFILE)

    def get_or_create_archiver(self, url: str, path: str, profile: dict) -> Any:
        """
        Method for getting or creating archiver object.
        :param url: Target URL.
        :param path: Local archive path.
        :param profile: Archiver profile.
        :return: Existing or newly created archiver object.
        """
        res = self._get("archiver", [FilterMask(
            [["url", "==", url], ["path", "==", path], ["profile", "==", profile]])])
        return res if res is not None else self._post("archiver", self.model["archiver"](url=url, path=path, profile=profile))

    def add_page(self, archiver: Any, url: str, path: str) -> None:
        """
        Method for adding page under archiver.
        :param archiver: Archiver object.
        :param url: Page URL.
        :param path: Local path.
        """
        res = self._get("page", [FilterMask(
            [["url", "==", url], ["path", "==", path]])])
        if res is None or res.archiver_id != archiver.id:
            res = self.model["page"](url=url, path=path)
            res.pages = archiver
            res = self._post("page", res)
        return res

    def add_asset(self, archiver_id: int, page_id: int, url: str, path: str) -> None:
        """
        Method for adding asset under archiver and page.
        :param archiver_id: Archiver ID.
        :param page_id: Page ID.
        :param url: Asset URL.
        :param path: Local path.
        """
        pass
