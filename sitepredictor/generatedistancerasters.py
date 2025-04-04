
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

POSTGRES_HOST                       = os.environ.get("POSTGRES_HOST")
POSTGRES_DB                         = os.environ.get("POSTGRES_DB")
POSTGRES_USER                       = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD                   = os.environ.get("POSTGRES_PASSWORD")
CLIPPING_PATH                       = '../uk--clipping.gpkg'

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

    output = subprocess.run(subprocess_array)

    # print("\n" + " ".join(subprocess_array) + "\n")

    if output.returncode != 0:
        LogError("subprocess.run failed with error code: " + str(output.returncode) + '\n' + " ".join(subprocess_array))
    return " ".join(subprocess_array)

def createFeaturesRaster(table, output):
    """
    Creates basic raster of features
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    LogMessage("Creating features raster from: " + table)

    if isfile(output): os.remove(output)

    runSubprocess([ "gdal_rasterize", \
                    "-burn", "1", \
                    "-tr", "250", "250", \
                    "-te", "54970", "7250", "654970", "1056000", \
                    "-a_nodata", "-9999", \
                    "-ot", "Float32", \
                    "-of", "GTiff", \
                    'PG:host=' + POSTGRES_HOST + ' user=' + POSTGRES_USER + ' password=' + POSTGRES_PASSWORD + ' dbname=' + POSTGRES_DB, \
                    "-sql", "SELECT ST_Transform(geom, 27700) FROM " + table, \
                    output])

def createDistanceRaster(input, output):
    """
    Creates distance raster from feature raster
    """

    global CLIPPING_PATH

    temp_file = 'temp.tif'
    temp_file2 = 'temp2.tif'

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

    # if isfile(temp_file): os.remove(temp_file)
    # if isfile(temp_file2): os.remove(temp_file2)

def createCappedDistanceRaster(input, output, capvalue):
    """
    Creates distance where values over 'capvalue' are set to 'capvalue'
    """

    LogMessage("Capping value on raster at " + str(capvalue) + " and outputting to: " + output)

    runSubprocess([ "gdal_calc", \
                    "-A", input, \
                    "--outfile=" + output, \
                    '--calc="(A*(A<=' + str(capvalue) + '))+(' + str(capvalue) + '*(A>' + str(capvalue) + '))"' ])


output_folder = "/Volumes/A002/Distance_Rasters/output/"
capped_distance = 30

rasters_to_generate = {
    'areas_of_outstanding_natural_beauty__uk__pro': 'AONB',
    'a_roads__uk__pro': 'ARoads',
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
    'a_roads__uk__pro': 'PrimaryRoads',
    'ramsar_sites__uk__pro': 'RAMSAR',
    'railway_lines__uk__pro': 'Railway',
    'special_areas_of_conservation__uk__pro': 'SACS',
    'special_protection_areas__uk__pro': 'SPA',
    'sites_of_special_scientific_interest__uk__pro': 'SSSI',
    'separation_distance_from_residential__uk__pro': 'UrbanRegions'
}

for raster in rasters_to_generate.keys():
    feature_raster = output_folder + raster + '.tif'
    distance_raster = output_folder + rasters_to_generate[raster] + '.tif'
    capped_distance_raster = output_folder + rasters_to_generate[raster] + '_Trans.tif'
    createFeaturesRaster(raster, feature_raster)
    createDistanceRaster(feature_raster, distance_raster)
    createCappedDistanceRaster(distance_raster, capped_distance_raster, capped_distance)
    os.remove(feature_raster)


