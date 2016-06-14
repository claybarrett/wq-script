#-------------------------------------------------------------------------------
# Name:        Preprocessing WQ data
# Purpose:     Reads a directory and list, compares the two. Then Gets all the scenes
#               that are within the specified time window.
#
# Author:      clay barrett
#               clay.barrett@gmail.com
# Created:     08 nov 2015
# Copyright:   (c) clay barrett 2016
# Licence:     see License.txt
#-------------------------------------------------------------------------------

import os
import sys
import logging
import logging.config

try:
    import arcpy
except ImportError:
    print "You need arcpy installed"
    sys.exit ( -1 )
import wqu

# get utility modules
from wqu import list_find
from wqu import discover_files
from wqu import read_file_selection
from wqu import select_md_files
from wqu import parse_metadata
from wqu import select_images
from wqu import code_images
from wqu import write_csv_output

# get variables from file
from variables import *


def main():
    # Setup Logger
    logger = main_logger.getChild(__name__)

    # Find water quality sample file
    wq_file = wqu.discover_files(settings['WQ_Path'])
    logger.debug('WQ File(s): {}'.format(wq_file))

    # Find coordinate file (WQ Sample locations with GPS coords, optionally Study Area)
    coord_file = wqu.discover_files(settings['Coord Path'])
    logger.debug('Coordinate File(s): {}'.format(coord_file))

    # Read Found Files
    # WQ file has SiteId, name, wq sample data.
    # Coord file has SiteId, name(s), and Lat/Long
    if wq_file and coord_file:
        read_wq_file = wqu.read_file_selection(wq_file, wq_headers.values())
        logger.debug('read wq list: {}'.format(read_wq_file))

        read_coord_file = wqu.read_file_selection(coord_file, coord_headers.values())
        logger.debug('read coord list: {}'.format(read_coord_file))

        # Make uniq lake id list off Coordinate list.
        # Changing to WB ID
        lake_id_list = wqu.list_find(read_coord_file, coord_headers['lake id'], 0)[1]
        unique_lake_id_list = list(set([i for i in lake_id_list]))
        logger.debug('unique lake id list: {}'.format(sorted(unique_lake_id_list)))

        # get list of files names in the LS metadata dir
        id_md_list = wqu.select_md_files(settings['Metadata_Path'], unique_lake_id_list)

        # with a list of files and completion, start reading through the data
        scene_selection = wqu.parse_metadata(unique_lake_id_list, read_wq_file, id_md_list)
        logger.debug('scene selection list: {}'.format(scene_selection))

        # ok so now we have huge lists
        selected_images = wqu.select_images(scene_selection)
        coded_selected_images = wqu.code_images(selected_images)

        #Write
        wqu.write_csv_output(coded_selected_images, working_dir, settings['Image List'])
        logger.debug('Fin')

    else:
        logger.debug('no wq file {} or coord file {} found'.format(wq_file, coord_file))


if __name__ == '__main__':
    # make sure the log dir exists, if not, make
    if not os.path.exists(settings['Log Path']):
        os.makedirs(settings['Log Path'])
    # setup logger
    log_ini = os.path.abspath(os.path.join(working_dir, script_dir, settings['Log Config']))
    logging.config.fileConfig(log_ini, defaults={'logfilename': os.path.join(settings['Log Path'], settings['Log Name'])})
    main_logger = logging.getLogger(os.path.basename(__file__).split('.')[0])

    main()
