#-------------------------------------------------------------------------------
# Name:        variables store
# Purpose:
#
# Author:      clay barrett
#
# Created:     01/may/2016
# Copyright:   (c) clay barrett 2016
# Licence:
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details <http://www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------
import os
from os.path import join
from datetime import timedelta

# cannot call things in the dict to make other things in the dict, so predefine these
# working_dir needs to be up one level from script_dir
#working_dir = os.getcwd()
working_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
script_dir = 'scripts'
log_dir = 'LOGS'
input_dir = 'Inputs'
wq_dir = 'WQ Data'
shape_dir = 'Lake Shapes'
# drop?
Original_Path = ""
Problem_List = []
# OWRB Sample site coordinates
coord_dir = 'Sample Sites'
lsmetadata_dir = 'LS Metadata'
scene_dir = 'LS Images'
output_dir = 'PROCESSED'
extract_dir = 'EXTRACTS'
# Intermediaries
box_dir = 'LAKE BOXES'
# Directory for shp with each lakes coords
point_dir = 'LAKE POINTS'
#image_list = 'Scene Order List.csv'

settings = {
# file types this script can read
'Input_Types': ['.csv', '.txt', '.shp'],
# Log config file name (in script dir)
'Log Config': 'log.ini',
#'Log Path': (os.path.join(working_dir, script_dir, log_dir)),
'Log Path': (os.path.join(working_dir, log_dir)),
'Log Name': 'logs.txt',
# Output from Preprocessing
'Image List': 'Scene Order List.csv',
#'Image List Path': (os.path.join(working_dir, image_list)),
# Location of the WQ file
'WQ_Path': (os.path.join(working_dir, input_dir, wq_dir)),
# Sample location GPS points... which is an Attribute table export from ArcMap
# This export is failing for my key feild... wbid, so reading direct from dbf?
'Lake Shape Path': (os.path.join(working_dir, input_dir, shape_dir)),
'Coord Path': (os.path.join(working_dir, input_dir, coord_dir)),
'Box Path': (os.path.join(working_dir, input_dir, box_dir)),
'Point Path': (os.path.join(working_dir, input_dir, point_dir)),
'Scene Path': (os.path.join(working_dir, input_dir, scene_dir)),
'Extract Path': (os.path.join(working_dir, extract_dir)),
'Output Path': (os.path.join(working_dir, output_dir)),
# LS scene info folder
'Metadata_Path': (os.path.join(working_dir, input_dir, lsmetadata_dir)),
## Preprocessing's Output
#'Output_Name': 'Scene_Selection.csv',
# Preprocessing's Output
'Output_Name': 'Scene_Selection.csv',
'Output_Headers': ['Site ID', 'Site Name', 'Inputs Read', 'WQ Sample Dates', 'Scene Name', \
    'Scene Date', 'date object of Scene Date'],
'date_allowance': timedelta(days=1),
'Coded_Headers': ['Code', 'Site ID', 'Site Name', 'Scene Name', 'Scene Date'],
# From ImageReadAndClip
# reading master/image list
'Code Header': 'Code',
'Scene Header': 'Scene Name',
# images we do not need to process >> Flip to desired bands, then passable to MD reader
'Pick Bands': ['1', '2', '3', '4', '5', '6', '7'],
#'Skip Bands': ['B1.TIF', 'B7.TIF', 'B8.TIF'],
'Parameter List': ['dnmin', 'gain', 'bias', 'lmax', 'lmin', 'qc_lmax', 'qc_lmin'],
## data to read in metadata file
##'Metadata Terms': [],
# The MinimumBoundaryGeometery "box" suffix
'Box Suffix': '_box.shp',
# Sample point coord file suffix
'Point Suffix': '_pnts.shp',
# metadata store
'MD file': 'metadata.pickle',
# from CorrectClips
'Cloud Dir': 'cloud mask',
'Agg File': 'agg.dbf',   # Extract file name
'Agg Output': 'Aggregated.csv',
'SD File': 'd.csv',     # Solar distance file from NASA
# UTM Window size
'Sample Window Size': 3,   # Optional parameter, default is 3. Must be odd.
'Clip Prefix': 'c_',
'Rad Prefix': 'r',  # Converted to TOA radiance
'Corr Prefix': 'c', # COST DOS corrected
'Temp Prefix': 't', # For B6 once it's temperature
'NDWI Prefix': 'water_', # prefix for water mask
'Blue Band': 'B1',
'Green Band': 'B2',
'Red Band': 'B3',
'NIR Band': 'B4',
'SWIR Band': 'B5',
'Thermal Band': 'B6',
# From Recombobulator
#'Image List': 'Master List.csv',
#'Processed Dir': 'Output', > output dir
#'Processed List': 'Aggregated.csv', > agg output
#'Bump Data': 'Bump Data.csv', > wq path?
'Exclude Site': 'B',
#'Threshold': 1.0,                   # Percent of sites required to report
#'date allowance': timedelta(days=1),
'Final Output': 'recombobulated.csv'
}

# empty dict... global... bad idea
sun_d = {}

Correction_Parameters = {
#dnmin, bias, gain, lmax, lmin, qc_lmax, qc_lmin
}

# from ImageReadAndClip... need better descriptions
lake_headers = {
'lake id': 'WBID',
'lake name': 'SHORT_NAME'
}

coord_headers = {
'lake id': 'WB_ID'
}

# Headers from WQ file we wish to read
wq_headers = {
#'lake id': 'WB_ID',
'id_header': 'Station ID',
'name_header': 'Station Description',
'date_header': 'Sample Date'
}

# Headers from file we wish to read with coordinates
coord_headers = {
'lake id': 'WB_ID',
'site_header': 'SITE_ID',
'name_header': 'DESCRIPTIO',
'lat_header': 'LATITUDE',
'long_header': 'LONGITUDE',
#'select_header': 'STUDY_AREA'
}

lake_headers = {
'lake id': 'WBID',
'lake name': 'SHORT_NAME'
}

# quick and dirty: name and day
metadata_headers = {
'scene_name': 'Landsat Scene Identifier',
'date_taken': 'Date Acquired'
}
