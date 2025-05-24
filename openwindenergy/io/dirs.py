from os import makedirs
from pathlib import Path


def make_folder(folderpath: Path):
    """
    Make folder if it doesn't already exist
    """
    if not folderpath.exists():
        makedirs(folderpath)
