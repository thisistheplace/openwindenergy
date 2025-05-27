import multiprocessing as mp
from pathlib import Path
import json

LOG = mp.get_logger()


def load_json(json_path: str | Path):
    """
    Gets contents of JSON file
    """
    json_path = Path(json_path)
    if not json_path.exists():
        msg = f"JSON file does not exist: {json_path}"
        LOG.error(msg)
        raise FileNotFoundError(msg)

    with open(json_path, "r") as json_file:
        return json.load(json_file)
