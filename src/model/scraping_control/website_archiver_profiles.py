# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""

ENTITY_PROFILE = {
    "archiver": {
        "#meta": {
            "schema": "archiving",
            "description": "Website archivers.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the archiver."
        },
        "url": {
            "type": "text",
            "required": True,
            "description": "URL of target website of the archiver."
        },
        "path": {
            "type": "text",
            "description": "Local saving path of the target data.",
        },
        "created": {
            "type": "datetime",
            "description": "Timestamp of creation.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "updated": {
            "type": "datetime",
            "description": "Timestamp of last update.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "patch": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "delete": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "inactive": {
            "type": "char",
            "description": "Flag for marking inactive entries.",
            "delete": "lambda _: 'X'"
        }
    },
    "page": {
        "#meta": {
            "schema": "archiving",
            "description": "Page of a target website.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the page."
        },
        "url": {
            "type": "text",
            "description": "API URL for fetching metadata.",
        },
        "path": {
            "type": "text",
            "description": "Local saving path of the target data.",
        },
        "created": {
            "type": "datetime",
            "description": "Timestamp of creation.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "updated": {
            "type": "datetime",
            "description": "Timestamp of last update.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "patch": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "delete": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "inactive": {
            "type": "char",
            "description": "Flag for marking inactive entries.",
            "delete": "lambda _: 'X'"
        }
    },
    "asset": {
        "#meta": {
            "schema": "archiving",
            "description": "Asset of target website.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the asset."
        },
        "url": {
            "type": "text",
            "description": "API URL for fetching metadata.",
        },
        "path": {
            "type": "text",
            "description": "Local saving path of the target data.",
        },
        "created": {
            "type": "datetime",
            "description": "Timestamp of creation.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "updated": {
            "type": "datetime",
            "description": "Timestamp of last update.",
            "post": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "patch": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')",
            "delete": "lambda _: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')"
        },
        "inactive": {
            "type": "char",
            "description": "Flag for marking inactive entries.",
            "delete": "lambda _: 'X'"
        }
    }
}

LINKAGE_PROFILE = {
    "pages": {
        "source": "archiver",
        "target": "page",
        "relation": "1:n",
        "linkage_type": "foreign_key",
        "source_key": [
            "int",
            "id"
        ],
        "target_key": [
            "int",
            "id"
        ]
    },
    "assets": {
        "source": "page",
        "target": "asset",
        "relation": "n:n",
        "linkage_type": "foreign_key",
        "source_key": [
            "int",
            "id"
        ],
        "target_key": [
            "int",
            "id"
        ]
    },

}

VIEW_PROFILE = {

}
