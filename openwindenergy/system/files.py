from pathlib import Path
import json


def load_json(json_path: str | Path):
    """
    Gets contents of JSON file
    """

    with open(json_path, "r") as json_file:
        return json.load(json_file)
