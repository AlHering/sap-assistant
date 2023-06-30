
from typing import List, Any
from src.configuration import configuration as cfg
from src.model.scraping_control.website_archiver_profiles import ENTITY_PROFILE, LINKAGE_PROFILE, VIEW_PROFILE
from src.utility.gold.sqlalchemy_entity_data_interface import SQLAlchemyEntityInterface as DBInterface
from src.utility.gold.filter_mask import FilterMask


class ModelDatabase(DBInterface):
    """
    Class, representing ACA Database.
    """

    def __init__(self) -> None:
        """
        Initiation method.
        """
        super().__init__(environment_profile={
            "backend": "database",
            "framework": "sqlalchemy",
            "arguments": {
                "database": cfg.ENV["DB_URL"],
                "dialect": cfg.ENV["DB_DIALECT"],
                "encoding": "utf-8"
            },
            "targets": "*",
            "handle_as_objects": True
        },
            entity_profiles=ENTITY_PROFILE,
            linkage_profiles=LINKAGE_PROFILE,
            view_profiles=VIEW_PROFILE)
