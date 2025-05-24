import multiprocessing as mp
from psycopg2.extensions import AsIs

from ..constants import *
from ..postgis import tables as pgistables

LOG = mp.get_logger()


def get_country_from_area(area: str):
    """
    Determine country that area is in using OSM_BOUNDARIES_GPKG
    """
    osm_boundaries_table = pgistables.reformat_table_name_absolute(OSM_BOUNDARIES)
    countries = [OSM_NAME_CONVERT[country] for country in OSM_NAME_CONVERT.keys()]

    results = pgistables.get_results(
        """
    WITH primaryarea AS
    (
        SELECT geom FROM %s WHERE (name = %s) OR (council_name = %s) LIMIT 1
    )
    SELECT 
        name, 
        ST_Area(ST_Intersection(primaryarea.geom, secondaryarea.geom)) geom_intersection 
    FROM %s secondaryarea, primaryarea 
    WHERE name = ANY (%s) AND ST_Intersects(primaryarea.geom, secondaryarea.geom) ORDER BY geom_intersection DESC LIMIT 1;
    """,
        (
            AsIs(osm_boundaries_table),
            area,
            area,
            AsIs(osm_boundaries_table),
            countries,
        ),
    )

    containing_country = results[0][0]

    for canonical_country in OSM_NAME_CONVERT.keys():
        if OSM_NAME_CONVERT[canonical_country] == containing_country:
            return canonical_country

    return None


def process_clipping_area(clipping_area: str):
    """
    Process custom clipping area

    :params clipping_area: name of clipping area
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_TABLE_PREFIX

    countries = ["england", "scotland", "wales", "northern-ireland"]

    if clipping_area.lower() == "uk":
        return CUSTOM_CONFIGURATION  # The default setup so change nothing
    if clipping_area.lower().replace(" ", "-") in countries:
        country = clipping_area.lower()
    else:
        country = get_country_from_area(clipping_area)

    if CUSTOM_CONFIGURATION is None:
        CUSTOM_CONFIGURATION = {"configuration": "--clip " + clipping_area}
    CUSTOM_CONFIGURATION["clipping"] = [clipping_area]

    if "areas" not in CUSTOM_CONFIGURATION:
        CUSTOM_CONFIGURATION["areas"] = [country, "uk"]
    elif country not in CUSTOM_CONFIGURATION:
        CUSTOM_CONFIGURATION["areas"].append(country)

    LOG.info("Custom clipping area: Clipping on '" + clipping_area + "'")
    LOG.info(
        "Custom clipping area: Selecting country-specific datasets for '"
        + country
        + "'"
    )
    LOG.info(
        "Custom clipping area: Note all generated tables for custom configuration will have '"
        + CUSTOM_CONFIGURATION_TABLE_PREFIX
        + "' prefix"
    )
    LOG.info(
        "Custom clipping area: Dropping previous custom configuration database tables"
    )
    pgistables.drop_custom_tables()

    return CUSTOM_CONFIGURATION
