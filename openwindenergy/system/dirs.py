import multiprocessing as mp
from os import makedirs, listdir, scandir
from pathlib import Path
import shutil

LOG = mp.get_logger()


def make_folder(folderpath: Path):
    """
    Make folder if it doesn't already exist
    """
    folderpath = Path(folderpath)
    if not folderpath.exists():
        makedirs(folderpath)


def list_files(folderpath: Path):
    """
    Get list of all files in folder
    Create folder if it doesn't exist
    """
    folderpath = Path(folderpath)
    make_folder(folderpath)
    files = [
        f
        for f in listdir(folderpath)
        if ((f != ".DS_Store") and (folderpath / f).is_file())
    ]
    if files is not None:
        files.sort()
    return files


def delete_dir_contents(folder: Path):
    """
    Deletes contents of folder but keep folder - needed for when docker compose manages folder mappings
    """
    folder = Path(folder)
    if not folder.is_dir():
        return

    files = list_files(folder)
    for file in files:
        (folder / file).unlink()

    subfolders = [Path(f.path) for f in scandir(folder) if f.is_dir()]

    for subfolder in subfolders:
        if len(subfolder.absolute) < len(folder) or not subfolder.absolute.startswith(
            folder
        ):
            msg = "Attempting to delete folder outside selected folder, aborting"
            LOG.error(msg)
            raise PermissionError(msg)
        shutil.rmtree(subfolder.absolute)
