import multiprocessing as mp
import psycopg2
from psycopg2.extensions import AsIs

from ..constants import *

LOG = mp.get_logger()


def pgis_exec(sql_text, sql_parameters):
    """
    Executes SQL statement
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=5,
        keepalives_count=5,
    )
    cur = conn.cursor()
    cur.execute(sql_text, sql_parameters)
    conn.commit()
    conn.close()


def drop_table(table_name):
    """
    Drops PostGIS table
    """

    pgis_exec("DROP TABLE IF EXISTS %s", (AsIs(table_name),))


def get_results(sql_text, sql_parameters):
    """
    Runs database query and returns results
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(sql_text, sql_parameters)
    results = cur.fetchall()
    conn.close()
    return results


def get_custom_tables():
    """
    Gets list of all custom configuration tables in database
    """

    global CUSTOM_CONFIGURATION_TABLE_PREFIX

    custom_configuration_prefix_escape = CUSTOM_CONFIGURATION_TABLE_PREFIX.replace(
        r"_", r"\_"
    )

    return get_results(
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

    for table in customtables:
        (table_name,) = table
        LOG.info(f" --> Dropping custom table: {table_name}")
        drop_table(table_name)


def get_all_tables():
    """
    Gets list of all tables
    """

    all_tables = get_results(
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

    global OSM_BOUNDARIES

    # ignore_tables = [reformatTableName(OSM_BOUNDARIES)]
    ignore_tables = []
    all_tables = get_all_tables()

    for table in all_tables:
        if table in ignore_tables:
            continue
        drop_table(table)


def get_derived_tables():
    """
    Gets list of all derived tables in database
    """

    # Derived tables:
    # Any 'buf'fered
    # Any 'pro'cessed
    # Any final layer 'tip_...'

    return get_results(
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

    for table in derivedtables:
        (table_name,) = table
        drop_table(table_name)


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

    return get_results(
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

    for table in legacytables:
        (table_name,) = table
        LOG.info("Removing legacy table: " + table_name)
        drop_table(table_name)


def drop_amalgamated_tables():
    """
    Drops all amalgamated tables in schema
    """

    LOG.info(" --> Dropping all tip_... tables")

    derivedtables = get_amalgamated_tables()

    for table in derivedtables:
        (table_name,) = table
        drop_table(table_name)


def get_legacy_tables():
    """
    Gets list of all legacy tables in database
    """

    # Legacy tables:
    # public_roads_a_and_b_roads_and_motorways__uk
    # tipheight_...

    return get_results(
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
