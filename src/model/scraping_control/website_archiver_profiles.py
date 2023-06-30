# -*- coding: utf-8 -*-
"""
****************************************************
*                  SAP Assistant                   *
*            (c) 2023 Alexander Hering             *
****************************************************
"""

ENTITY_PROFILE = {
    "model": {
        "#meta": {
            "schema": "machine_learning_models",
            "description": "Machine Learning model.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the model."
        },
        "metadata": {
            "type": "json",
            "description": "Metadata of the model.",
        },
        "api_url": {
            "type": "text",
            "description": "API URL for fetching metadata.",
        },
        "source": {
            "type": "str",
            "description": "Metadata source.",
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
    "model_version": {
        "#meta": {
            "schema": "machine_learning_models",
            "description": "Local Machine Learning model version.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the model version."
        },
        "metadata": {
            "type": "json",
            "description": "Metadata of the model version.",
        },
        "api_url": {
            "type": "text",
            "description": "API URL for fetching metadata.",
        },
        "source": {
            "type": "str",
            "description": "Metadata source.",
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
    "model_file": {
        "#meta": {
            "schema": "machine_learning_models",
            "description": "Local Machine Learning model file.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the model file."
        },
        "file_name": {
            "type": "str",
            "required": True,
            "description": "File name, consisting of name and extension.",
        },
        "folder": {
            "type": "text",
            "required": True,
            "description": "Folder of model file.",
        },
        "sha256": {
            "type": "str",
            "description": "SHA256 hash of the file."
        },
        "status": {
            "type": "str",
            "required": True,
            "description": "Status of the model file: 'unknown' -> 'linked' -> 'collected' -> 'tracked'",
            "post": "lambda _: 'unknown'"
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
            "schema": "machine_learning_models",
            "description": "Local Machine Learning model assets.",
            "keep_deleted": True
        },
        "id": {
            "type": "int",
            "key": True,
            "autoincrement": True,
            "required": True,
            "description": "ID of the asset."
        },
        "type": {
            "type": "str",
            "required": True,
            "description": "Asset type.",
        },
        "path": {
            "type": "text",
            "required": True,
            "description": "File path of the asset.",
        },
        "sha256": {
            "type": "str",
            "description": "SHA256 hash of the file."
        },
        "metadata": {
            "type": "json",
            "required": True,
            "description": "Metadata of the asset.",
        },
        "status": {
            "type": "str",
            "required": True,
            "description": "Status of the model file: 'collected' -> 'downloaded'",
            "post": "lambda _: 'collected'"
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
    "link": {
        "source": "model_file",
        "target": "model_version",
        "relation": "1:1",
        "linkage_type": "foreign_key",
        "source_key": [
            "int",
            "id"
        ],
        "target_key": [
            "int",
            "id"
        ]
    }

}

VIEW_PROFILE = {

}
