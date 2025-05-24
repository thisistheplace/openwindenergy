import multiprocessing as mp
from pathlib import Path

from ..constants import *
from ..io.dirs import get_dir_files, delete_dir_contents
from ..postgis import tables as pgistables

LOG = mp.get_logger()


def purge_all():
    """
    Deletes all database tables and build folder
    """

    global WORKING_FOLDER, BUILD_FOLDER, TILESERVER_FOLDER, OSM_DOWNLOADS_FOLDER, OSM_EXPORT_DATA, OSM_CONFIG_FOLDER, DATASETS_DOWNLOADS_FOLDER

    pgistables.drop_all_tables()

    tileserver_folder_name = TILESERVER_FOLDER.parent.name
    build_files = get_dir_files(BUILD_FOLDER)
    for build_file in build_files:
        # Don't delete log files from BUILD_FOLDER
        if not build_file.endswith(".log"):
            os.remove(BUILD_FOLDER + build_file)
    osm_files = get_dir_files(OSM_DOWNLOADS_FOLDER)
    for osm_file in osm_files:
        os.remove(OSM_DOWNLOADS_FOLDER + osm_file)
    tileserver_files = get_dir_files(TILESERVER_FOLDER)
    for tileserver_file in tileserver_files:
        os.remove(TILESERVER_FOLDER + tileserver_file)

    pwd = os.path.dirname(os.path.realpath(__file__))

    # Delete items in BUILD_FOLDER

    subfolders = [Path(f.path) for f in os.scandir(BUILD_FOLDER) if f.is_dir()]
    absolute_build_folder = os.path.abspath(BUILD_FOLDER)

    for subfolder in subfolders:

        # Don't delete 'postgres' folder as managed by separate docker instance
        # Don't delete 'tileserver' folder yet as some elements are managed separately
        # Don't delete 'landcover' and 'coastline' folders as managed by docker compose
        if subfolder.name in [
            "postgres",
            tileserver_folder_name,
            "coastline",
            "landcover",
        ]:
            continue

        subfolder_absolute = os.path.abspath(subfolder)

        if len(subfolder_absolute) < len(
            absolute_build_folder
        ) or not subfolder_absolute.startswith(absolute_build_folder):
            msg = "Attempting to delete folder outside build folder, aborting"
            LOG.error(msg)
            raise PermissionError(msg)

        shutil.rmtree(subfolder_absolute)

    # Delete all items in 'landcover' and 'coastline' folders but keep folders in case managed by docker

    delete_dir_contents(WORKING_FOLDER / "coastline")
    delete_dir_contents(WORKING_FOLDER / "landcover")

    # Delete selected items in TILESERVER_FOLDER

    subfolders = [Path(f.path) for f in os.scandir(TILESERVER_FOLDER) if f.is_dir()]
    absolute_tileserver_folder = os.path.abspath(TILESERVER_FOLDER)

    for subfolder in subfolders:

        # Don't delete 'fonts' as this is created by openwindenergy-fonts
        if subfolder.name in ["fonts"]:
            continue

        subfolder_absolute = os.path.abspath(subfolder)

        if len(subfolder_absolute) < len(
            absolute_tileserver_folder
        ) or not subfolder_absolute.startswith(absolute_tileserver_folder):
            msg = "Attempting to delete folder outside tileserver folder, aborting"
            LOG.error(msg)
            raise PermissionError(msg)

        shutil.rmtree(subfolder_absolute)
