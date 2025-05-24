from datetime import datetime
import multiprocessing as mp
import os
from os.path import isfile
import time
import typer
from typing import Optional

from .constants import *
from .postgis.manager import PostGisManager
from .logging import init_logging
from .io.dirs import make_folder

LOG = mp.get_logger()

def main(
        height_to_tip: Optional[float],
        
):
    """
    Run openwindenergy

    python3 openwindenergy.py
    - Uses default values for turbine height-to-tip and blade-radius values.

    python3 openwindenergy.py [HEIGHT TO TIP]
    - Where 'HEIGHT TO TIP' is height-to-tip in metres of target wind turbine. Uses default blade-radius.

    python3 openwindenergy.py [HEIGHT TO TIP] [BLADE RADIUS]
    - Where 'HEIGHT TO TIP' is height-to-tip in metres and 'BLADE RADIUS' is blade-radius in metres of target wind turbine.

    Possible additional arguments:

    --custom               Supply custom YML configuration file. Can be local file path, internet url or name on CKAN open data portal (https://data.openwind.energy).
    --clip                 Supply custom area for clipping. Uses OSM name from osm-boundaries.gpkg. Overrides any 'clipping' setting in custom YML.
    --purgeall             Clear all downloads and database tables as if starting fresh.
    --purgedb              Clear all PostGIS tables and reexport final layer files.
    --purgederived         Clear all derived (ie. non-core data) PostGIS tables and reexport final layer files.
    --purgeamalgamated     Clear all amalgamated PostGIS tables and reexport final layer files.
    --skipdownload         Skip download stage and just do PostGIS processing.
    --skipfonts            Skip font installation stage and use hosted version of openmaptiles fonts.
    --regenerate dataset   Regenerate specific dataset by redownloading and recreating all tables relating to dataset.
    --buildtileserver      Rebuild files for tileserver.
    """

    global SERVER_BUILD, PROCESSING_START
    global BUILD_FOLDER, LOG_SINGLE_PASS, PROCESSING_COMPLETE_FILE, HEIGHT_TO_TIP, BLADE_RADIUS
    global CUSTOM_CONFIGURATION, REGENERATE_INPUT, REGENERATE_OUTPUT, PERFORM_DOWNLOAD, SKIP_FONTS_INSTALLATION
    global CKAN_URL, DATASETS_DOWNLOADS_FOLDER, PROCESSING_COMPLETE_FILE, PROCESSING_STATE_FILE

    PROCESSING_START = time.time()

    make_folder(BUILD_FOLDER)

    if SERVER_BUILD:
        if isfile(PROCESSING_COMPLETE_FILE):
            LOG.info("Previous build run complete, aborting this run")
            exit(0)

    time.sleep(3)

    LOG.info("""\033[1;34m
***********************************************************************
******************** OPEN WIND ENERGY DATA PIPELINE *******************
***********************************************************************
\033[0m""")

    with open('PROCESSINGSTART', 'w', encoding='utf-8') as file: 
        file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing started"))

    LOG.info("***********************************************************************")
    LOG.info("*************** Starting Open Wind Energy data pipeline ***************")
    LOG.info("***********************************************************************")
    LOG.info("")

    pg_manager = PostGisManager()
    pg_manager.connect()

    LOG.info("Processing command line arguments...")

    if len(sys.argv) > 1:
        arg_index, height_to_tip_set, blade_radius_set = 0, False, False
        for arg in sys.argv:
            arg = arg.strip()
            if isfloat(arg):
                if not height_to_tip_set:
                    HEIGHT_TO_TIP = float(arg)
                    height_to_tip_set = True
                    LOG.info("************ Using HEIGHT_TO_TIP value: " + formatValue(HEIGHT_TO_TIP) + ' metres ************')
                else:
                    BLADE_RADIUS = float(arg)
                    blade_radius_set = True
                    LOG.info("************ Using BLADE_RADIUS value: " + formatValue(BLADE_RADIUS) + ' metres ************')

            if arg == '--custom':
                if len(sys.argv) > arg_index:
                    customconfig = sys.argv[arg_index + 1]
                    LOG.info("--custom argument passed: Using custom configuration '" + customconfig + "'")
                    CUSTOM_CONFIGURATION = processCustomConfiguration(customconfig)

            if arg == '--clip':
                if len(sys.argv) > arg_index:
                    # *** This will override any 'clipping' setting in '--custom', above
                    clippingarea = sys.argv[arg_index + 1]
                    LOG.info("--clip argument passed: Using custom clipping area '" + clippingarea + "'")
                    CUSTOM_CONFIGURATION = processClippingArea(clippingarea)

            if arg == '--purgeall':
                LOG.info("--purgeall argument passed: Clearing database and all build files")
                REGENERATE_INPUT = True
                REGENERATE_OUTPUT = True
                purgeAll()

            if arg == '--purgedb':
                LOG.info("--purgedb argument passed: Clearing database")
                REGENERATE_INPUT = True
                REGENERATE_OUTPUT = True
                postgisDropAllTables()

            if arg == '--purgederived':
                LOG.info("--purgederived argument passed: Clearing derived database tables")
                REGENERATE_OUTPUT = True
                postgisDropDerivedTables()

            if arg == '--purgeamalgamated':
                LOG.info("--purgeamalgamated argument passed: Clearing amalgamated database tables")
                REGENERATE_OUTPUT = True
                postgisDropAmalgamatedTables()

            if arg == '--skipdownload':
                LOG.info("--skipdownload argument passed: Skipping download stage")
                PERFORM_DOWNLOAD = False

            if arg == '--skipfonts':
                LOG.info("--skipfonts argument passed: Skipping font installation and using hosted CDN fonts")
                SKIP_FONTS_INSTALLATION = True

            if arg == '--buildtileserver':
                LOG.info("--buildtileserver argument passed: Building files required for tileserver")
                buildTileserverFiles()
                exit()

            if arg == '--regenerate':
                if len(sys.argv) > arg_index:
                    regeneratedataset = sys.argv[arg_index + 1]
                    LOG.info("--regenerate argument passed: Redownloading and rebuilding all tables related to " + regeneratedataset)
                    deleteDatasetAndAncestors(regeneratedataset)

            if arg == '--help':
                print("""
Command syntax:

python3 openwindenergy.py
- Uses default values for turbine height-to-tip and blade-radius values.

python3 openwindenergy.py [HEIGHT TO TIP]
- Where 'HEIGHT TO TIP' is height-to-tip in metres of target wind turbine. Uses default blade-radius.

python3 openwindenergy.py [HEIGHT TO TIP] [BLADE RADIUS]
- Where 'HEIGHT TO TIP' is height-to-tip in metres and 'BLADE RADIUS' is blade-radius in metres of target wind turbine.

Possible additional arguments:

--custom               Supply custom YML configuration file. Can be local file path, internet url or name on CKAN open data portal (https://data.openwind.energy).
--clip                 Supply custom area for clipping. Uses OSM name from osm-boundaries.gpkg. Overrides any 'clipping' setting in custom YML.
--purgeall             Clear all downloads and database tables as if starting fresh.
--purgedb              Clear all PostGIS tables and reexport final layer files.
--purgederived         Clear all derived (ie. non-core data) PostGIS tables and reexport final layer files.
--purgeamalgamated     Clear all amalgamated PostGIS tables and reexport final layer files.
--skipdownload         Skip download stage and just do PostGIS processing.
--skipfonts            Skip font installation stage and use hosted version of openmaptiles fonts.
--regenerate dataset   Regenerate specific dataset by redownloading and recreating all tables relating to dataset.
--buildtileserver      Rebuild files for tileserver.

""")
                exit()

            arg_index += 1

    initPipeline(rebuildCommandLine(sys.argv))

    if PERFORM_DOWNLOAD:
        downloadDatasets(CKAN_URL, DATASETS_DOWNLOADS_FOLDER)

    runProcessingOnDownloads(DATASETS_DOWNLOADS_FOLDER)
    
    # Set up status flag files

    with open(PROCESSING_COMPLETE_FILE, 'w') as file: file.write("PROCESSINGCOMPLETE")
    if isfile(PROCESSING_STATE_FILE): os.remove(PROCESSING_STATE_FILE)

if __name__ == "__main__":
    init_logging()

    # Only remove log file on main thread
    if isfile(LOG_SINGLE_PASS): os.remove(LOG_SINGLE_PASS)
    
    app = typer.Typer()
    app.command()(main)
    app()