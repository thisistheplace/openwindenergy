import multiprocessing as mp
import psycopg2
from psycopg2.extensions import AsIs

from .manager import Db
from ..constants import *
from ..standardise import reformat_dataset_name
from ..system.process import run_subprocess

LOG = mp.get_logger()


def get_custom_tables():
    """
    Gets list of all custom configuration tables in database
    """
    custom_configuration_prefix_escape = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace(
        r"_", r"\_"
    )

    return Db().fetch_all(
        r"""
    SELECT tables.table_name
    FROM information_schema.tables
    WHERE 
    table_catalog=%s AND 
    table_schema='public' AND 
    table_type='BASE TABLE' AND
    table_name NOT IN ('spatial_ref_sys') AND 
    table_name LIKE '"""
        + custom_configuration_prefix_escape
        + r"""%%' 
    ORDER BY table_name;
    """,
        (POSTGRES_DB,),
    )


def drop_custom_tables():
    """
    Drops all custom configuration tables in schema
    """

    customtables = get_custom_tables()

    db = Db()
    for table in customtables:
        (table_name,) = table
        LOG.info(f" --> Dropping custom table: {table_name}")
        db.drop_table(table_name)


def get_all_tables():
    """
    Gets list of all tables
    """

    all_tables = Db().fetch_all(
        r"""
    SELECT tables.table_name
    FROM information_schema.tables
    WHERE 
    table_catalog=%s AND 
    table_schema='public' AND 
    table_type='BASE TABLE' AND
    table_name NOT IN ('spatial_ref_sys')
    ORDER BY table_name;
    """,
        (POSTGRES_DB,),
    )
    return [table[0] for table in all_tables]


def drop_all_tables():
    """
    Drops all tables in schema
    """

    ignore_tables = []
    all_tables = get_all_tables()

    db = Db()
    for table in all_tables:
        if table in ignore_tables:
            continue
        db.drop_table(table)


def get_derived_tables():
    """
    Gets list of all derived tables in database
    """

    # Derived tables:
    # Any 'buf'fered
    # Any 'pro'cessed
    # Any final layer 'tip_...'

    return Db().get_results(
        r"""
    SELECT tables.table_name
    FROM information_schema.tables
    WHERE 
    table_catalog=%s AND 
    table_schema='public' AND 
    table_type='BASE TABLE' AND
    table_name NOT IN ('spatial_ref_sys') AND
    (
        (table_name LIKE '%%\_\_buf\_%%') OR 
        (table_name LIKE '%%\_\_pro') OR 
        (table_name LIKE 'tip\_%%') 
    )
    ORDER BY table_name;
    """,
        (POSTGRES_DB,),
    )


def drop_derived_tables():
    """
    Drops all derived tables in schema
    """

    LOG.info(" --> Dropping all tip_*, *__pro and *__buf_* tables")

    derivedtables = get_derived_tables()

    db = Db()
    for table in derivedtables:
        (table_name,) = table
        db.drop_table(table_name)


def reformat_table_name_absolute(name):
    """
    Reformats names, eg. dataset names, ignoring custom settings (so absolute) to be compatible with Postgres
    Different from 'reformatTableName' which will add CUSTOM_CONFIGURATION_TABLE_PREFIX if using custom configuration
    """

    return name.replace(".gpkg", "").replace("-", "_")


def get_amalgamated_tables():
    """
    Gets list of all amalgamated tables in database
    """

    return Db().get_results(
        r"""
    SELECT tables.table_name
    FROM information_schema.tables
    WHERE 
    table_catalog=%s AND 
    table_schema='public' AND 
    table_type='BASE TABLE' AND
    table_name NOT IN ('spatial_ref_sys') AND
    table_name LIKE 'tip\_%%';
    """,
        (POSTGRES_DB,),
    )


def postgisDropLegacyTables():
    """
    Drops all legacy tables in schema
    """
    legacytables = get_legacy_tables()

    db = Db()
    for table in legacytables:
        (table_name,) = table
        LOG.info("Removing legacy table: " + table_name)
        db.drop_table(table_name)


def drop_amalgamated_tables():
    """
    Drops all amalgamated tables in schema
    """

    LOG.info(" --> Dropping all tip_... tables")
    derivedtables = get_amalgamated_tables()

    db = Db()
    for table in derivedtables:
        (table_name,) = table
        db.drop_table(table_name)


def get_legacy_tables():
    """
    Gets list of all legacy tables in database
    """

    # Legacy tables:
    # public_roads_a_and_b_roads_and_motorways__uk
    # tipheight_...

    return Db().get_results(
        r"""
    SELECT tables.table_name
    FROM information_schema.tables
    WHERE 
    table_catalog=%s AND 
    table_schema='public' AND 
    table_type='BASE TABLE' AND
    table_name NOT IN ('spatial_ref_sys') AND
    (
        (table_name LIKE 'public\_roads\_a\_and\_b\_roads\_and\_motorways\_\_uk%%') OR
        (table_name LIKE 'tipheight\_%%')
    );
    """,
        (POSTGRES_DB,),
    )


def reformat_table_name(name):
    """
    Reformats names, eg. dataset names, to be compatible with Postgres
    Also adds in CUSTOM_CONFIGURATION_TABLE_PREFIX in case we're using custom configuration file§
    """

    global CUSTOM_CONFIGURATION

    table = reformat_table_name_absolute(name)

    if CUSTOM_CONFIGURATION is not None:
        if not table.startswith(CUSTOM_CONFIGURATION_TABLE_PREFIX):
            table = CUSTOM_CONFIGURATION_TABLE_PREFIX + table

    return table


def buildBufferTableName(layername, buffer):
    """
    Builds buffer table name
    """

    return reformat_table_name(layername) + "__buf_" + buffer.replace(".", "_") + "m"


def buildProcessedTableName(layername):
    """
    Builds processed table name
    """

    return reformat_table_name(layername) + "__pro"


def build_union_table_name(layername):
    """
    Builds union table name
    """

    return reformat_table_name(layername) + "__union"


def get_final_layer_latest_name(table_name):
    """
    Gets latest name from table name, eg. 'tip-135m-bld-40m--ecology-and-wildlife...' -> 'latest--ecology-and-wildlife...'
    If CUSTOM_CONFIGURATION, add CUSTOM_CONFIGURATION_FILE_PREFIX
    """

    global CUSTOM_CONFIGURATION

    custom_configuration_prefix = ""
    if CUSTOM_CONFIGURATION is not None:
        custom_configuration_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    dataset_name = reformat_dataset_name(table_name)
    elements = dataset_name.split("--")
    if len(elements) > 1:
        latest_name = (
            custom_configuration_prefix
            + LATEST_OUTPUT_FILE_PREFIX
            + "--".join(elements[1:])
        )
    else:
        latest_name = (
            custom_configuration_prefix + LATEST_OUTPUT_FILE_PREFIX + dataset_name
        )

    return latest_name


def create_grid_clipped_file(table_name, core_dataset_name, file_path):
    """
    Create grid clipped version of file to improve rendering and performance when used as mbtiles
    """
    scratch_table_1 = "_scratch_table_1"
    output_grid = reformat_table_name(OUTPUT_GRID_TABLE)

    db = Db()
    if db.table_exists(scratch_table_1):
        db.drop_table(scratch_table_1)

    db.exec(
        "CREATE TABLE %s AS SELECT (ST_Dump(ST_Intersection(layer.geom, grid.geom))).geom geom FROM %s layer, %s grid;",
        (
            AsIs(scratch_table_1),
            AsIs(table_name),
            AsIs(output_grid),
        ),
    )

    inputs = run_subprocess(
        [
            "ogr2ogr",
            file_path,
            "PG:host="
            + POSTGRES_HOST
            + " user="
            + POSTGRES_USER
            + " password="
            + POSTGRES_PASSWORD
            + " dbname="
            + POSTGRES_DB,
            "-overwrite",
            "-nln",
            core_dataset_name,
            scratch_table_1,
            "-s_srs",
            WORKING_CRS,
            "-t_srs",
            "EPSG:4326",
        ]
    )

    if db.table_exists(scratch_table_1):
        db.drop_table(scratch_table_1)
