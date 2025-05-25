from datetime import datetime
import multiprocessing as mp
import os
import time
import typer
from typing_extensions import Annotated

from .config import process_custom_config
from .constants import *
from .format import format_float
from .postgis.manager import PostGisManager
from .postgis import tables as pgistables
from .logging import init_logging
from .system.dirs import make_folder
from .workflow.clipping_area import process_clipping_area
from .workflow.purge import purge_all

LOG = mp.get_logger()


def main(
    height_to_tip: Annotated[
        float,
        typer.Argument(
            help="Height to blade tip of target wind turbine, in metres. Uses default blade-radius."
        ),
    ] = HEIGHT_TO_TIP,
    blade_radius: Annotated[
        float,
        typer.Argument(
            help="Blade radius of target wind turbine, in metres. Uses default if not provided."
        ),
    ] = BLADE_RADIUS,
    custom_config: Annotated[
        str,
        typer.Argument(
            help="Path to custom YML configuration file. Can be local file path, internet url or name on CKAN open data portal (https://data.openwind.energy)."
        ),
    ] = None,
    clipping_area: Annotated[
        str,
        typer.Argument(
            help="Name of custom area for clipping. Uses OSM name from osm-boundaries.gpkg. Overrides any 'clipping' setting in custom YML."
        ),
    ] = None,
    regenerate_dataset: Annotated[
        str,
        typer.Argument(
            help="Regenerate specific named dataset by redownloading and recreating all tables relating to dataset."
        ),
    ] = None,
    purge_all: Annotated[
        bool,
        typer.Option(
            help="Clear all downloads and database tables as if starting fresh."
        ),
    ] = False,
    purge_db: Annotated[
        bool,
        typer.Option(help="Clear all PostGIS tables and reexport final layer files."),
    ] = False,
    purge_derived: Annotated[
        bool,
        typer.Option(
            help="Clear all derived (ie. non-core data) PostGIS tables and reexport final layer files."
        ),
    ] = False,
    purge_amalgamated: Annotated[
        bool,
        typer.Option(
            help="Clear all amalgamated PostGIS tables and reexport final layer files."
        ),
    ] = False,
    skip_download: Annotated[
        bool, typer.Option(help="Skip download stage and just do PostGIS processing.")
    ] = False,
    skip_fonts: Annotated[
        bool,
        typer.Option(
            help="Skip font installation stage and use hosted version of openmaptiles fonts."
        ),
    ] = False,
    build_tile_server: Annotated[
        bool, typer.Option(help="Rebuild files for tileserver.")
    ] = None,
):
    """
    CLI to run openwindenergy
    """

    global PROCESSING_START, CUSTOM_CONFIGURATION, REGENERATE_INPUT, REGENERATE_OUTPUT, SKIP_FONTS_INSTALLATION
    global HEIGHT_TO_TIP, BLADE_RADIUS

    PROCESSING_START = time.time()

    make_folder(Path(BUILD_FOLDER))

    if SERVER_BUILD:
        if PROCESSING_COMPLETE_FILE.is_file():
            LOG.info("Previous build run complete, aborting this run")
            exit(0)

    time.sleep(3)

    LOG.info(
        """\033[1;34m
***********************************************************************
******************** OPEN WIND ENERGY DATA PIPELINE *******************
***********************************************************************
\033[0m"""
    )

    with open("PROCESSINGSTART", "w", encoding="utf-8") as file:
        file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing started"))

    LOG.info("***********************************************************************")
    LOG.info("*************** Starting Open Wind Energy data pipeline ***************")
    LOG.info("***********************************************************************")
    LOG.info("")

    pg_manager = PostGisManager()
    pg_manager.connect()

    LOG.info("Processing command line arguments...")

    if height_to_tip is not None:
        HEIGHT_TO_TIP = float(height_to_tip)
        LOG.info(
            f"************ Using HEIGHT_TO_TIP value: {format_float(HEIGHT_TO_TIP)} metres ************"
        )

    if blade_radius is not None:
        BLADE_RADIUS = float(blade_radius)
        LOG.info(
            f"************ Using BLADE_RADIUS value: {format_float(BLADE_RADIUS)} metres ************"
        )

    if custom_config is not None:
        LOG.info(
            f"--custom_config argument passed: Using custom configuration '{custom_config}'"
        )
        CUSTOM_CONFIGURATION = process_custom_config(custom_config)

    if clipping_area is not None:
        if custom_config is not None:
            LOG.warning(
                f"Clipping area '{clipping_area}' will override area defined in custom_config"
            )
        LOG.info(
            f"--clipping_area argument passed: Using custom clipping area '{clipping_area}'"
        )
        CUSTOM_CONFIGURATION = process_clipping_area(clipping_area)

    if purge_all:
        LOG.info("--purgeall argument passed: Clearing database and all build files")
        REGENERATE_INPUT = True
        REGENERATE_OUTPUT = True
        purge_all()

    if purge_db:
        LOG.info("--purgedb argument passed: Clearing database")
        REGENERATE_INPUT = True
        REGENERATE_OUTPUT = True
        pgistables.drop_all_tables()

    if purge_derived:
        LOG.info("--purgederived argument passed: Clearing derived database tables")
        REGENERATE_OUTPUT = True
        pgistables.drop_derived_tables()

    if purge_amalgamated:
        LOG.info(
            "--purgeamalgamated argument passed: Clearing amalgamated database tables"
        )
        REGENERATE_OUTPUT = True
        pgistables.drop_amalgamated_tables()

    if skip_fonts:
        LOG.info(
            "--skipfonts argument passed: Skipping font installation and using hosted CDN fonts"
        )
        SKIP_FONTS_INSTALLATION = True

    if build_tile_server:
        LOG.info(
            "--buildtileserver argument passed: Building files required for tileserver"
        )
        buildTileserverFiles()
        exit()

    if regenerate_dataset is not None:
        LOG.info(
            "--regenerate argument passed: Redownloading and rebuilding all tables related to "
            + regeneratedataset
        )
        deleteDatasetAndAncestors(regeneratedataset)

    initPipeline(rebuildCommandLine(sys.argv))

    if skip_download:
        LOG.info("--skipdownload argument passed: Skipping download stage")
        downloadDatasets(CKAN_URL, DATASETS_DOWNLOADS_FOLDER)

    runProcessingOnDownloads(DATASETS_DOWNLOADS_FOLDER)

    # Set up status flag files

    with open(PROCESSING_COMPLETE_FILE, "w") as file:
        file.write("PROCESSINGCOMPLETE")
    if PROCESSING_STATE_FILE.is_file():
        os.remove(PROCESSING_STATE_FILE)


if __name__ == "__main__":
    init_logging()

    # Only remove log file on main thread
    if LOG_SINGLE_PASS.is_file():
        os.remove(LOG_SINGLE_PASS)

    app = typer.Typer()
    app.command()(main)
    app()
