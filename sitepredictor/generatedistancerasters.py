
import sys
import logging
import csv
import json
# import geojson
import requests
import os
import urllib.request
import subprocess
import xmltodict
import shutil
import yaml
import sqlite3
import psycopg2
import psycopg2.extras 
import time
import pprint as pp
from psycopg2 import sql
from psycopg2.extensions import AsIs
from zipfile import ZipFile
from os import listdir, makedirs
from os.path import isfile, isdir, basename, join, exists
from dotenv import load_dotenv
from osgeo import gdal, osr, ogr

load_dotenv('../.env')

RASTER_RESOLUTION                   = 125 # Number of metres per raster grid square
RASTER_XMIN                         = 0 
RASTER_YMIN                         = 7250
RASTER_XMAX                         = 664000 
RASTER_YMAX                         = 1296000
POSTGRES_HOST                       = os.environ.get("POSTGRES_HOST")
POSTGRES_DB                         = os.environ.get("POSTGRES_DB")
POSTGRES_USER                       = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD                   = os.environ.get("POSTGRES_PASSWORD")
CLIPPING_PATH                       = '../uk--clipping.gpkg'
QUIET_MODE                          = True
OUTPUT_FOLDER                       = "/Volumes/A002/Distance_Rasters/output/"

logging.basicConfig(
    format='%(asctime)s [%(levelname)-2s] %(message)s',
    level=logging.INFO)


def LogMessage(logtext):
    """
    Logs message to console with timestamp
    """

    logging.info(logtext)

def LogError(logtext):
    """
    Logs error message to console with timestamp
    """

    logging.error("*** ERROR *** " + logtext)


def runSubprocess(subprocess_array):
    """
    Runs subprocess
    """

    global QUIET_MODE

    if QUIET_MODE: 
        if subprocess_array[0] in ['gdal_proximity.py']: subprocess_array.append('-q')
        if subprocess_array[0] in ['gdal_create', 'gdal_rasterize', 'gdalwarp', 'gdal_calc']: subprocess_array.append('--quiet')

    output = subprocess.run(subprocess_array)

    # print("\n" + " ".join(subprocess_array) + "\n")

    if output.returncode != 0:
        LogError("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))
    return " ".join(subprocess_array)

def reformatTableName(name):
    """
    Reformats names, eg. dataset names, to be compatible with Postgres
    """

    return name.replace('.gpkg', '').replace('.geojson', '').replace("-", "_")

# ***********************************************************
# ******************** PostGIS functions ********************
# ***********************************************************

def postgisCheckTableExists(table_name):
    """
    Checks whether table already exists
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    table_name = reformatTableName(table_name)
    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s);", (table_name, ))
    tableexists = cur.fetchone()[0]
    cur.close()
    return tableexists

def postgisExec(sql_text, sql_parameters=None):
    """
    Executes SQL statement
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, \
                            keepalives=1, keepalives_idle=30, keepalives_interval=5, keepalives_count=5)
    cur = conn.cursor()
    if sql_parameters is None: cur.execute(sql_text)
    else: cur.execute(sql_text, sql_parameters)
    conn.commit()
    conn.close()

# ***********************************************************
# ********************** GIS functions **********************
# ***********************************************************

def createBlankRaster(raster, output):
    """
    Creates blank raster
    """

    global RASTER_RESOLUTION, RASTER_XMIN, RASTER_YMIN, RASTER_XMAX, RASTER_YMAX

    if isfile(output): os.remove(output)

    LogMessage("Creating blank raster for: " + raster)

    runSubprocess([ "gdal_create", \
                    "-burn", "0", \
                    "-outsize", str(int((RASTER_XMAX - RASTER_XMIN) / RASTER_RESOLUTION)), str(int((RASTER_YMAX - RASTER_YMIN) / RASTER_RESOLUTION)), \
                    "-a_ullr", str(RASTER_XMIN), str(RASTER_YMIN), str(RASTER_XMAX), str(RASTER_YMAX), \
                    "-a_nodata", "-9999", \
                    "-ot", "Float64", \
                    "-of", "GTiff", \
                    "-a_srs", "EPSG:27700", \
                    output])


def createFeaturesRaster(table, output):
    """
    Creates basic raster of features
    """

    global RASTER_RESOLUTION, RASTER_XMIN, RASTER_YMIN, RASTER_XMAX, RASTER_YMAX
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    LogMessage("Creating features raster from: " + table)

    if isfile(output): os.remove(output)

    runSubprocess([ "gdal_rasterize", \
                    "-burn", "1", \
                    "-tr", str(RASTER_RESOLUTION), str(RASTER_RESOLUTION), \
                    "-te", str(RASTER_XMIN), str(RASTER_YMIN), str(RASTER_XMAX), str(RASTER_YMAX), \
                    "-a_nodata", "-9999", \
                    "-ot", "Float64", \
                    "-of", "GTiff", \
                    'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                    "-sql", "SELECT ST_Transform(geom, 27700) FROM " + table, \
                    output])

def createFeaturesRasterWithValue(table, output, value_column):
    """
    Creates basic raster of features
    """

    global RASTER_RESOLUTION, RASTER_XMIN, RASTER_YMIN, RASTER_XMAX, RASTER_YMAX
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    LogMessage("Creating features raster from: " + table)

    if isfile(output): os.remove(output)

    runSubprocess([ "gdal_rasterize", \
                    "-a", value_column, \
                    "-tr", str(RASTER_RESOLUTION), str(RASTER_RESOLUTION), \
                    "-te", str(RASTER_XMIN), str(RASTER_YMIN), str(RASTER_XMAX), str(RASTER_YMAX), \
                    "-a_nodata", "-9999", \
                    "-ot", "Float64", \
                    "-of", "GTiff", \
                    'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                    "-sql", "SELECT " + value_column + ", ST_Transform(geom, 27700) FROM " + table, \
                    output])

def cropRaster(input, output):
    """
    Crops raster to clipping path
    """

    global CLIPPING_PATH

    LogMessage("Cropping: " + output)

    if isfile(output): os.remove(output)

    runSubprocess([ "gdalwarp", \
                    "-of", "GTiff", \
                    "-cutline", "-dstnodata", "-9999", CLIPPING_PATH, \
                    "-crop_to_cutline", input, \
                    output ])

def createDistanceRaster(input, output):
    """
    Creates distance raster from feature raster
    """

    global CLIPPING_PATH, OUTPUT_FOLDER

    temp_file = OUTPUT_FOLDER + 'temp.tif'
    temp_file2 = OUTPUT_FOLDER + 'temp2.tif'

    if isfile(temp_file): os.remove(temp_file)
    if isfile(temp_file2): os.remove(temp_file2)
    if isfile(output): os.remove(output)

    LogMessage("Creating distance raster from: " + input)

    runSubprocess([ "gdal_proximity.py", \
                    input, temp_file, \
                    "-values", "1", \
                    "-distunits", "GEO"])

    LogMessage("Converting metres to kilometers for: " + input)

    runSubprocess([ "gdal_calc", \
                    "-A", temp_file, \
                    "--outfile=" + temp_file2, \
                    '--calc="((A>=0)*A/1000)"' ])

    LogMessage("Cropping distance to: " + output)

    runSubprocess([ "gdalwarp", \
                    "-of", "GTiff", \
                    "-cutline", "-dstnodata", "-9999", CLIPPING_PATH, \
                    "-crop_to_cutline", temp_file2, \
                    output ])

    if isfile(temp_file): os.remove(temp_file)
    if isfile(temp_file2): os.remove(temp_file2)

def createCappedDistanceRaster(input, output, capvalue):
    """
    Creates distance where values over 'capvalue' are set to 'capvalue'
    """

    LogMessage("Capping value on raster at " + str(capvalue) + " and outputting to: " + output)

    if isfile(output): os.remove(output)

    runSubprocess([ "gdal_calc", \
                    "-A", input, \
                    "--outfile=" + output, \
                    '--calc="(A*(A<=' + str(capvalue) + '))+(' + str(capvalue) + '*(A>' + str(capvalue) + '))"' ])

def createAgeMeanTable():
    """
    Creates AgeMean table
    """

    sql_clause = "("
    for age in range(0, 101):
        db_field = "age_" + str(age)
        if age == 0: db_field = "age_under_1"
        if age == 100: db_field = "age_100_over"
        sql_clause += "(" + str(age) + " * ages." + db_field + "::int)"
        if age != 100: sql_clause += " + "
    sql_clause += ")/ages.total::float AS val"

    age_mean_table = 'sitepredictor__census_2011_age_mean__geography__uk'

    if not postgisCheckTableExists(age_mean_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT %s, areas.geo_code, areas.geom 
        FROM sitepredictor__census_geography__uk areas, sitepredictor__census_2011_age__uk ages
        WHERE areas.geo_code = ages.geo_code; 
        """, (AsIs(age_mean_table), AsIs(sql_clause), ))

    return age_mean_table

def createPercOwnTable():
    """
    Creates Percentage Owned table
    """

    perc_owned_table = 'sitepredictor__census_2011_perc_owned__geography__uk'

    if not postgisCheckTableExists(perc_owned_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 100 * tenure.tenure_owned__prop val, areas.geo_code, areas.geom 
        FROM sitepredictor__census_geography__uk areas, sitepredictor__census_2011_tenure__uk tenure
        WHERE areas.geo_code = tenure.geo_code; 
        """, (AsIs(perc_owned_table), ))

    return perc_owned_table

def createQualPercL4Table():
    """
    Creates Percentage with L4 qualification table
    """

    qual_perc_L4_table = 'sitepredictor__census_2011_perc_qual_l4__geography__uk'

    if not postgisCheckTableExists(qual_perc_L4_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 100 * qualifications.qualifications_highest_qualification_level_4__prop val, areas.geo_code, areas.geom 
        FROM sitepredictor__census_geography__uk areas, sitepredictor__census_2011_qualifications__uk qualifications
        WHERE areas.geo_code = qualifications.geo_code; 
        """, (AsIs(qual_perc_L4_table), ))

    return qual_perc_L4_table

def createSocGrdABTable():
    """
    Creates Social Grade Classification AB table
    """

    soc_grd_ab_table = 'sitepredictor__census_2011_soc_grd_ab__geography__uk'

    if not postgisCheckTableExists(soc_grd_ab_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 
        (
            100 * 
            (
                occupation.occupation_1_managers__prop + 
                occupation.occupation_2_professional__prop
            )
        ) val, areas.geo_code, areas.geom 
        FROM sitepredictor__census_geography__uk areas, sitepredictor__census_2011_occupation__uk occupation
        WHERE areas.geo_code = occupation.geo_code; 
        """, (AsIs(soc_grd_ab_table), ))

    return soc_grd_ab_table

def createNearestTurbineBuilt(study_year):
    """
    Creates layer of all turbines operational before year 'study_year'
    """

    operational_turbines_table = 'sitepredictor__' + str(study_year) + '_operational_turbines__uk'

    if not postgisCheckTableExists(operational_turbines_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 
        projects.project_name,
        projects.geom
        FROM 
        windturbines_all_projects__uk projects
        WHERE 
        project_date_operational < %s AND
        project_status IN ('Operational', 'Awaiting Construction', 'Under Construction', 'Decommissioned', 'Planning Permission Expired');
        """, (AsIs(operational_turbines_table), str(study_year) + '-01-01', ))

    return operational_turbines_table

def createNearestTurbinePlanned(study_year):
    """
    Creates layer of all planned turbines before 'study_year'
    """

    planned_turbines_table = 'sitepredictor__' + str(study_year) + '_planned_turbines__uk'

    if not postgisCheckTableExists(planned_turbines_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 
        projects.project_name,
        projects.geom
        FROM 
        windturbines_all_projects__uk projects
        WHERE 
        project_date_end < %s AND
        project_status IN ('Revised', 'Application Submitted', 'Appeal Lodged', 'No Application Required');
        """, (AsIs(planned_turbines_table), str(study_year) + '-01-01', ))

    return planned_turbines_table

def createNearestTurbineRejected(study_year):
    """
    Creates layer of all rejected turbines before 'study_year'
    """

    rejected_turbines_table = 'sitepredictor__' + str(study_year) + '_rejected_turbines__uk'

    if not postgisCheckTableExists(rejected_turbines_table):

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT 
        projects.project_name,
        projects.geom
        FROM 
        windturbines_all_projects__uk projects
        WHERE 
        project_date_end < %s AND
        project_status IN ('Application Refused', 'Abandoned', 'Appeal Withdrawn', 'Application Withdrawn', 'Appeal Refused');
        """, (AsIs(rejected_turbines_table), str(study_year) + '-01-01', ))

    return rejected_turbines_table

capped_distance = 30
study_year = '2015'

blank_rasters = ['tenure_owned', 'no_of_turbines', 'urban_large']
non_distance_rasters = ['political', 'age_mean', 'perc_owned', 'perc_qual_l4', 'soc_grd_ab']

age_mean_table = createAgeMeanTable()
perc_owned_table = createPercOwnTable()
qual_perc_L4 = createQualPercL4Table()
soc_grd_ab = createSocGrdABTable()
nearest_turbine_built = createNearestTurbineBuilt(study_year)
nearest_turbine_planned = createNearestTurbinePlanned(study_year)
nearest_turbine_rejected = createNearestTurbineRejected(study_year)

rasters_to_generate = {
    'tenure_owned': 'TenureOwned',
    'no_of_turbines': 'No..of.Turbines',
    'urban_large': 'UrbanLarge',
    age_mean_table: 'AgeMean',
    perc_owned_table: 'PercOwn',
    qual_perc_L4: 'QualPercL4',
    nearest_turbine_built: 'NearestTurbineBuilt',
    nearest_turbine_planned: 'NearestTurbinePlanned',
    nearest_turbine_rejected: 'NearestTurbineRejected',
    'sitepredictor__councils_political_' + str(study_year) + '_con__uk': 'Con_share',
    'sitepredictor__councils_political_' + str(study_year) + '_lab__uk': 'Lab_share',
    'sitepredictor__councils_political_' + str(study_year) + '_ld__uk': 'LD_share',
    'sitepredictor__councils_political_' + str(study_year) + '_other__uk': 'Oth_share',
    'windspeeds_noabl__uk': 'WindSpeed45',
    'areas_of_outstanding_natural_beauty__uk__pro': 'AONB',
    'a_roads__uk__pro': ['ARoads', 'PrimaryRoads'],
    'civilian_airports__uk__pro': 'Airports',
    'b_roads__uk__pro': 'BRoads',
    'heritage_coasts__uk__pro': 'HCoast',
    'power_hv_lines__uk__pro': 'HVpowerline',
    'mod_training_areas__uk__pro': 'MilitarySites',
    'minor_roads__uk__pro': 'MinRoads',
    'motorways__uk__pro': 'Motorways',
    'national_nature_reserves__uk__pro': 'NNR',
    'national_parks__uk__pro': 'NationalParks',
    'power_lines__uk__pro': 'Powerlines',
    'ramsar_sites__uk__pro': 'RAMSAR',
    'railway_lines__uk__pro': 'Railway',
    'special_areas_of_conservation__uk__pro': 'SACS',
    'special_protection_areas__uk__pro': 'SPA',
    'sites_of_special_scientific_interest__uk__pro': 'SSSI',
    'separation_distance_from_residential__uk__pro': 'UrbanRegions'
}

# rasters_to_generate = {
#     'tenure_owned': 'TenureOwned',
# }

# Create political datasets

political_parties = ['con', 'lab', 'ld', 'other']

for political_party in political_parties:

    political_geometries_table = 'sitepredictor__councils_political_' + str(study_year) + '_' + political_party + '__uk'

    if not postgisCheckTableExists(political_geometries_table):
        
        # if political_party == 'other':

        #     postgisExec("""
        #     CREATE TABLE %s AS 
        #     SELECT (100 * (political.other::float + political.nat::float)/political.total::float) val, political.area area, councils.geom geom 
        #     FROM sitepredictor__councils__uk councils, sitepredictor__political__uk political
        #     WHERE political.area = councils.name 
        #     AND political.year = %s
        #     ORDER BY ST_Area(ST_Transform(councils.geom, 3857)) DESC; 
        #     """, (AsIs(political_geometries_table), study_year, ))

        #     postgisExec("""
        #     INSERT INTO %s 
        #     SELECT 
        #     (SELECT (100 * SUM(other::float + nat::float) / SUM(total::float)) FROM sitepredictor__political__uk WHERE year = %s GROUP BY year) val, 
        #     councils.name area, councils.geom geom FROM sitepredictor__councils__uk councils LEFT JOIN sitepredictor__political__uk political
        #     ON councils.name = political.area 
        #     WHERE political.area IS NULL;
        #     """, (AsIs(political_geometries_table), study_year, ))

        # else:

        postgisExec("""
        CREATE TABLE %s AS 
        SELECT (100 * political.%s::float/political.total::float) val, political.area area, councils.geom geom 
        FROM sitepredictor__councils__uk councils, sitepredictor__political__uk political
        WHERE political.area = councils.name 
        AND political.year = %s
        ORDER BY ST_Area(ST_Transform(councils.geom, 3857)) DESC; 
        """, (AsIs(political_geometries_table), AsIs(political_party), study_year, ))

        postgisExec("""
        INSERT INTO %s 
        SELECT 
        (SELECT (100 * SUM(%s::float) / SUM(total::float)) FROM sitepredictor__political__uk WHERE year = %s GROUP BY year) val, 
        councils.name area, councils.geom geom FROM sitepredictor__councils__uk councils LEFT JOIN sitepredictor__political__uk political
        ON councils.name = political.area 
        WHERE political.area IS NULL;
        """, (AsIs(political_geometries_table), AsIs(political_party), study_year, ))


temp_raster = OUTPUT_FOLDER + 'temp.tif'

for raster in rasters_to_generate.keys():
    feature_raster = OUTPUT_FOLDER + raster + '.tif'

    # Lookup for raster can be single value or array
    # If single value, convert to array and iterate through it

    output_list = rasters_to_generate[raster]
    if isinstance(output_list, str): output_list = [output_list]

    for output in output_list:
        output_raster = OUTPUT_FOLDER + output + '.tif'
        capped_distance_raster = OUTPUT_FOLDER + output + '_Trans.tif'

        raster_done = False
        for nondistance_raster in non_distance_rasters:
            if nondistance_raster in raster:
                createFeaturesRasterWithValue(raster, temp_raster, 'val')
                cropRaster(temp_raster, output_raster)
                raster_done = True
                
        if not raster_done:
            if raster in blank_rasters:
                createBlankRaster(output_raster, temp_raster)
                cropRaster(temp_raster, output_raster)
            elif 'windspeed' in raster:
                createFeaturesRasterWithValue(raster, temp_raster, 'windspeed')
                cropRaster(temp_raster, output_raster)
            else:
                createFeaturesRaster(raster, feature_raster)
                createDistanceRaster(feature_raster, output_raster)
                createCappedDistanceRaster(output_raster, capped_distance_raster, capped_distance)
                os.remove(feature_raster)

        if isfile(temp_raster): os.remove(temp_raster)



