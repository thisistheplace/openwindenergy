import os
import csv
import pyproj
import json
import psycopg2
import psycopg2.extras 
from psycopg2.extensions import AsIs
from dotenv import load_dotenv

load_dotenv('../.env')

POSTGRES_HOST                       = os.environ.get("POSTGRES_HOST")
POSTGRES_DB                         = os.environ.get("POSTGRES_DB")
POSTGRES_USER                       = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD                   = os.environ.get("POSTGRES_PASSWORD")

def ingrs_to_lnglat(easting, northing):
    transformer = pyproj.Transformer.from_crs("EPSG:29903", "EPSG:4326")
    lat, lng = transformer.transform(easting, northing)
    return lng, lat

def bngrs_to_lnglat(easting, northing):
    transformer = pyproj.Transformer.from_crs("EPSG:27700", "EPSG:4326")
    lat, lng = transformer.transform(easting, northing)
    return lng, lat

def lnglat_to_bngrs(longitude, latitude):
    crs_source = pyproj.CRS("EPSG:4326")  # WGS84 (longitude/latitude)
    crs_destination = pyproj.CRS("EPSG:27700")  # British National Grid

    transformer = pyproj.Transformer.from_crs(crs_source, crs_destination, always_xy=True)
    easting, northing = transformer.transform(longitude, latitude)
    return easting, northing

def postgisGetResults(sql_text, sql_parameters=None):
    """
    Runs database query and returns results
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor()
    if sql_parameters is None: cur.execute(sql_text)
    else: cur.execute(sql_text, sql_parameters)
    results = cur.fetchall()
    conn.close()
    return results

def postgisGetResultsAsDict(sql_text, sql_parameters=None):
    """
    Runs database query and returns results
    """

    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    conn = psycopg2.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if sql_parameters is None: cur.execute(sql_text)
    else: cur.execute(sql_text, sql_parameters)
    results = cur.fetchall()
    conn.close()
    return results

def getStatusSummary(status):

    status = status.strip()

    if status in ['Operational', 'Awaiting Construction', 'Under Construction', 'Decommissioned', 'Planning Permission Expired']: return 'Approved'
    if status in ['Application Refused', 'Abandoned', 'Appeal Withdrawn', 'Application Withdrawn', 'Appeal Refused']: return 'Refused/Abandoned'
    if status in ['Revised', 'Application Submitted', 'Appeal Lodged', 'No Application Required']: return 'Submitted'
    
    print("Shouldn't be here - quit")
    exit()

    return None

def loadRepdLookup():
    global file_repd

    repd_lookup = {}
    with open(file_repd, 'r', newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        output_row = {}
        index = 0
        for row in reader:
            repd_lookup['REPD:' + str(row['Ref ID'])] = row
    
    return repd_lookup

def convertToKilometers(value):
    if value is None: return None
    return int(value) / 1000

def getNearestTurbineStats(position, guid, start_date):

    if start_date.strip() == '': return {
        'NearestTurbineBuilt': None,
        'NearestTurbinePlanned': None,
        'NearestTurbineRejected': None
    }

    resultsNearestTurbineBuilt = postgisGetResults("""
    SELECT 
    MIN
    (
        ST_Distance
        (
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 3857),
            ST_Transform(geom, 3857)
        )
    )
    FROM 
    windturbines_all_projects__uk 
    WHERE 
    project_guid != %s AND 
    project_date_operational < %s AND
    project_status IN ('Operational', 'Awaiting Construction', 'Under Construction', 'Decommissioned', 'Planning Permission Expired');
    """, (AsIs(position['lng']), AsIs(position['lat']), guid, start_date, ))

    if len(resultsNearestTurbineBuilt) == 0: resultsNearestTurbineBuilt = None
    else: resultsNearestTurbineBuilt = convertToKilometers(resultsNearestTurbineBuilt[0][0])

    resultsNearestTurbinePlanned = postgisGetResults("""
    SELECT 
    MIN
    (
        ST_Distance
        (
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 3857),
            ST_Transform(geom, 3857)
        )
    )
    FROM 
    windturbines_all_projects__uk 
    WHERE 
    project_guid != %s AND 
    project_date_end < %s AND
    project_status IN ('Revised', 'Application Submitted', 'Appeal Lodged', 'No Application Required');
    """, (AsIs(position['lng']), AsIs(position['lat']), guid, start_date, ))

    if len(resultsNearestTurbinePlanned) == 0: resultsNearestTurbinePlanned = None
    else: resultsNearestTurbinePlanned = convertToKilometers(resultsNearestTurbinePlanned[0][0])

    resultsNearestTurbineRejected = postgisGetResults("""
    SELECT 
    MIN
    (
        ST_Distance
        (
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 3857),
            ST_Transform(geom, 3857)
        )
    )
    FROM 
    windturbines_all_projects__uk 
    WHERE 
    project_guid != %s AND 
    project_date_end < %s AND
    project_status IN ('Application Refused', 'Abandoned', 'Appeal Withdrawn', 'Application Withdrawn', 'Appeal Refused');
    """, (AsIs(position['lng']), AsIs(position['lat']), guid, start_date, ))

    if len(resultsNearestTurbineRejected) == 0: resultsNearestTurbineRejected = None
    else: resultsNearestTurbineRejected = convertToKilometers(resultsNearestTurbineRejected[0][0])

    return {
        'NearestTurbineBuilt': resultsNearestTurbineBuilt,
        'NearestTurbinePlanned': resultsNearestTurbinePlanned,
        'NearestTurbineRejected': resultsNearestTurbineRejected
    }

def getAuthority(position):
    pass


file_input = 'finaldata.csv'
file_output = 'TurbineFullInfo_NewData.csv'
file_repd = 'repd-q3-oct-2024.csv'

field_conversion = {
    'Site.Name': 'project_name',	
    'No..of.Turbines': 'project_numturbines',
    'Turbine.Capacity..MW.': 'REPD:Turbine Capacity (MW)',
    'Capacity': 'REPD:Installed Capacity (MWelec)',
    'Status.Summary': 'SPECIALCASE',
    'Ref_ID': 'REPD:Ref ID',
    'Record.Last.Updated..dd.mm.yyyy.': 'REPD:Record Last Updated (dd/mm/yyyy)',
    'County': 'REPD:County',
    'Region': 'REPD:Region',
    'Country': 'turbine_country',
    'Planning.Application.Submitted': 'REPD:Planning Application Submitted',
    'Under.Construction': 'REPD:Under Construction',
    'Operational': 'REPD:Operational',
    'Size': 'SPECIALCASE',
    'year': 'SPECIALCASE',
    'yearCategory': 'SPECIALCASE',
    'Planning_Status_Summary': 'SPECIALCASE',
    'NearestTurbineBuilt': 'SPECIALCASE',
    'NearestTurbinePlanned': 'SPECIALCASE',
    'NearestTurbineRejected': 'SPECIALCASE',
    'lon': 'turbine_lnglat_lng',
    'lat': 'turbine_lnglat_lat',
    'Airports': 'distance__civilian_airports__uk',
    'AONB': 'distance__areas_of_outstanding_natural_beauty__uk',
    'ARoads': 'distance__a_roads__uk',
    'BRoads': 'distance__b_roads__uk',
    'HCoast': 'distance__heritage_coasts__uk',
    'HVpowerline': 'distance__power_hv_lines__uk',
    'MilitarySites': 'distance__mod_training_areas__uk',
    'MinRoads': 'distance__minor_roads__uk',
    'Motorways': 'distance__motorways__uk',
    'NationalParks': 'distance__national_parks__uk',
    'NNR': 'distance__national_nature_reserves__uk',
    'Powerlines': 'distance__power_lines__uk',
    'PrimaryRoads': 'distance__a_roads__uk',
    'Railway': 'distance__railway_lines__uk',
    'RAMSAR': 'distance__ramsar_sites__uk',
    'SACS': 'distance__special_areas_of_conservation__uk',
    'SPA': 'distance__special_protection_areas__uk',
    'SSSI': 'distance__sites_of_special_scientific_interest__uk',
    'UrbanLarge': 'distance__separation_distance_from_residential__uk',
    'UrbanRegions': 'distance__separation_distance_from_residential__uk',
    'UrbanSmall': 'distance__separation_distance_from_residential__uk',
    'Slope': 'ZERO',
    'UKElevation': 'turbine_elevation',
    'WindSpeed45': 'windspeed',
    'CensusMerged': 'BLANK',
    'AgeTotal': 'age_total',
    'AgeMean': 'age_mean',
    'AgeMedian': 'age_median',
    'QualTotal': 'qualifications_total',
    'QualLevel4': 'SPECIALCASE',
    'QualPercL4': 'SPECIALCASE',
    'SocGrdTot': 'occupation_total',
    'SocGrdAB': 'SPECIALCASE',
    'TenureTotal': 'tenure_total',
    'TenureOwned': 'SPECIALCASE',
    'PercOwn': 'SPECIALCASE',
    'Year': 'SPECIALCASE',
    'Authority': 'REPD:Planning Authority',
    'Code': 'BLANK',
    'Type': 'ZERO',
    'Council_si': 'political_total',
    'Con': 'political_con',
    'Lab': 'political_lab',
    'LD': 'political_ld',
    'Other': 'political_other',
    'SNP_PC': 'political_nat',
    'Control': 'political_majority',
    'Majority': 'ZERO',
    'Con_share': 'political_proportion_con',
    'Lab_share': 'political_proportion_lab',
    'LD_share': 'political_proportion_ld',
    'Oth_share': 'political_proportion_other',
    'SNP_PC_sha': 'political_proportion_nat'
}

Planning_Status_Summary = {
    'Approved': '1',
    'Refused/Abandoned': '0',
    'Submitted': 'NA',
}

repd_lookup = loadRepdLookup()

output_data = []
with open(file_input, 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    index = 0
    
    for row in reader:

        # if index < 2100:
        #     index += 1
        #     continue

        # print(index)

        output_row = {}
        easting, northing = lnglat_to_bngrs(float(row['turbine_lnglat_lng']), float(row['turbine_lnglat_lat']))
        row['distance__power_lines__uk'] = float(row['distance__power_lv_lines__uk'])
        if float(row['distance__power_hv_lines__uk']) < row['distance__power_lines__uk']: row['distance__power_lines__uk'] = float(row['distance__power_hv_lines__uk'])

        output_row['Turbine_ID'] = row['ogc_fid']
        output_row['X.coordinate'] = easting
        output_row['Y.coordinate'] = northing

        status_summary = getStatusSummary(row['project_status'])
        capacity = repd_lookup[row['project_guid']]['Installed Capacity (MWelec)']
        nearestTurbineStats = getNearestTurbineStats({'lng': float(row['turbine_lnglat_lng']), 'lat': float(row['turbine_lnglat_lat'])}, row['project_guid'], row['project_date_end'])

        size = 'Small'
        if (capacity.strip() != ''):
            if (float(capacity) >= 1): size = 'Large'
        if status_summary is None: continue

        for key in field_conversion.keys():
            if key == 'Status.Summary': output_row[key] = status_summary
            if key == 'Size': output_row[key] = size
            if key == 'year': 
                # project_date_end is date of last planning application decision - and so most relevant to prediction
                output_row[key] = row['project_date_end'][0:4]
                output_row['yearCategory'] = 'Before 2010'
                if (output_row['year'] == '') or (int(output_row['year']) > 2010): output_row['yearCategory'] = '2010 Onwards'
            if key == 'Year': output_row[key] = output_row['year']
            if key == 'Planning_Status_Summary': output_row[key] = Planning_Status_Summary[output_row['Status.Summary']]
            if key == 'QualLevel4':
                output_row[key] = int(int(row['qualifications_total']) * float(row['qualifications_highest_qualification_level_4_proportional']))
                output_row['QualPercL4'] = int(100 * float(row['qualifications_highest_qualification_level_4_proportional']))
            if key == 'TenureOwned':
                output_row[key] = int(int(row['tenure_total']) * float(row['tenure_owned_proportional']))
                output_row['PercOwn'] = int(100 * float(row['tenure_owned_proportional']))
            if key == 'SocGrdAB':
                socgrdAB = 0
                socgrdAB += float(row['occupation_1_managers_proportional'])
                socgrdAB += float(row['occupation_2_professional_proportional'])	
                output_row['SocGrdAB'] = 100 * socgrdAB
            if 'NearestTurbine' in key: output_row[key] = nearestTurbineStats[key]
            if key == 'Country':
                if row[field_conversion[key]].strip() != repd_lookup[row['project_guid']]['Country']:
                    print("Record's country (according to coordinates) does not match REPD version of Country - potential manual data error. Index:", index, row['project_guid'])

            if field_conversion[key] != '':
                converted_field = field_conversion[key]

                if converted_field == 'ZERO': output_row[key] = 0
                if converted_field == 'BLANK': output_row[key] = ''

                if converted_field not in ['SPECIALCASE', 'ZERO', 'BLANK']:
                    if converted_field.startswith("REPD:"):
                        converted_field = converted_field.replace('REPD:', '')
                        output_row[key] = repd_lookup[row['project_guid']][converted_field]
                    else:
                        output_row[key] = row[converted_field]

                        # If political share, convert proportion to percentage
                        if ('_share' in key) or (key == 'SNP_PC_sha'): 
                            if (output_row[key].strip() == ''): output_row[key] = 0
                            else: output_row[key] = 100 * float(output_row[key])
                        
                        # If 'distance__' then convert metres to kilometres
                        if 'distance__' in converted_field: output_row[key] = float(output_row[key]) / 1000

            if key in ['Under.Construction', 'Operational']:
                if output_row[key] == '': output_row[key] = 'NA'

        if index % 100 == 0:
            print("Computed", index)

        output_data.append(output_row)
        index += 1

        # if index > 10: break

with open(file_output, 'w', newline='') as csvfile:
    fieldnames = output_data[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

with open(file_output, 'a', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    for output_row in output_data:
        writer.writerow(output_row)
