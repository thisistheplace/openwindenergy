import multiprocessing as mp

from ..constants import *
from ..system.dirs import make_folder
from ..system.files import load_json
from ..system.process import run_subprocess

LOG = mp.get_logger()


def download_osm_data():
    """
    Downloads core OSM data
    """

    global BUILD_FOLDER, OSM_MAIN_DOWNLOAD, OSM_DOWNLOADS_FOLDER, TILEMAKER_DOWNLOAD_SCRIPT, TILEMAKER_COASTLINE, TILEMAKER_LANDCOVER, TILEMAKER_COASTLINE_CONFIG

    make_folder(BUILD_FOLDER)
    make_folder(OSM_DOWNLOADS_FOLDER)

    if not (OSM_DOWNLOADS_FOLDER / Path(OSM_MAIN_DOWNLOAD).name).is_file():

        LOG.info("Downloading latest OSM data")

        # Download to temp file in case download interrupted for any reason, eg. user clicks 'Stop processing'

        download_temp = OSM_DOWNLOADS_FOLDER / "temp.pbf"
        if download_temp.is_file():
            os.remove(download_temp)

        run_subprocess(["wget", OSM_MAIN_DOWNLOAD, "-O", download_temp])

        shutil.copy(download_temp, OSM_DOWNLOADS_FOLDER / Path(OSM_MAIN_DOWNLOAD).name)
        if download_temp.is_file():
            os.remove(download_temp)

    LOG.info("Checking all files required for OSM tilemaker...")

    shp_extensions = ["shp", "shx", "dbf", "prj"]
    tilemaker_config_json = load_json(TILEMAKER_COASTLINE_CONFIG)
    tilemaker_config_layers = list(tilemaker_config_json["layers"].keys())

    all_tilemaker_layers_downloaded = True
    for layer in tilemaker_config_layers:
        layer_elements = tilemaker_config_json["layers"][layer]
        if "source" in layer_elements:
            for shp_extension in shp_extensions:
                source_file = layer_elements["source"].replace(
                    ".shp", "." + shp_extension
                )
                if not Path(source_file).is_file():
                    LOG.info("Missing file for OSM tilemaker: " + source_file)
                    all_tilemaker_layers_downloaded = False

    if all_tilemaker_layers_downloaded:
        LOG.info("All files downloaded for OSM tilemaker")
    else:
        LOG.info("Downloading global water and coastline data for OSM tilemaker")
        run_subprocess([TILEMAKER_DOWNLOAD_SCRIPT])
