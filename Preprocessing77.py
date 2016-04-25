#-------------------------------------------------------------------------------
# Name:        Preprocessing WQ data
# Purpose:     Reads a directory and list, compares the two. Then Gets all the scenes
#               that are within the specified time window.
#
# Author:      clay barrett
#               clay.barrett@gmail.com
# Created:     08 nov 2015
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
try:
    import arcpy
except ImportError:
    print "You need arcpy installed"
    sys.exit ( -1 )

import os
import sys
import time
import logging
import logging.config

# get sub modules
import flatten
import list_find
import objectify_dates
import discover_files
import read_file_selection
import select_md_files
import parse_metadata
import select_images
import code_images
import write_csv_output

# get variables from file
from variables import *


def main():
    # Setup Logger
    logger = main_logger.getChild(__name__)

    # Find water quality sample file
    wq_file = discover_files.main(settings['WQ_Path'])
    logger.debug('WQ File(s): {}'.format(wq_file))

    # Find coordinate file (WQ Sample locations with GPS coords, optionally Study Area)
    coord_file = discover_files.main(settings['Coord Path'])
    logger.debug('Coordinate File(s): {}'.format(coord_file))

    # Read Found Files
    # WQ file has SiteId, name, wq sample data.
    # Coord file has SiteId, name(s), and Lat/Long
    if wq_file and coord_file:
        read_wq_file = read_file_selection.main(wq_file, wq_headers.values())
        logger.debug('read wq list: {}'.format(read_wq_file))

        read_coord_file = read_file_selection.main(coord_file, coord_headers.values())
        logger.debug('read coord list: {}'.format(read_coord_file))

        # Make uniq lake id list off Coordinate list.
        # Changing to WB ID
        lake_id_list = list_find.main(read_coord_file, coord_headers['lake id'], 0)[1]
        unique_lake_id_list = list(set([i for i in lake_id_list]))
        logger.debug('unique lake id list: {}'.format(sorted(unique_lake_id_list)))

        # get list of files names in the LS metadata dir
        id_md_list = select_md_files.main(settings['Metadata_Path'], unique_lake_id_list)

        #logger.debug('discovered dir list: {}'.format(dir_list))
        # what's missing? and does 45, 7a, and 7b appear?

        # with a list of files and completion, start reading through the data
        scene_selection = parse_metadata.main(unique_lake_id_list, read_wq_file, id_md_list)
        logger.debug('scene selection list: {}'.format(scene_selection))

        # ok so now we have huge lists
        selected_images = select_images.main(scene_selection)
        coded_selected_images = code_images.main(selected_images)

        #Write
        write_csv_output.main(coded_selected_images, working_dir, settings['Image List'])
        logger.debug('Fin')

    else:
        logger.debug('no wq file {} or coord file {} found'.format(wq_file, coord_file))


if __name__ == '__main__':
    # make sure the log dir exists, if not, make
    if not os.path.exists(settings['Log Path']):
        os.makedirs(settings['Log Path'])
    # setup logger
    log_ini = os.path.abspath( os.path.join(working_dir, script_dir, settings['Log Config']))
    logging.config.fileConfig(log_ini, defaults={'logfilename': settings['Log Name']})
    main_logger = logging.getLogger(os.path.basename(__file__).split('.')[0])

    main()
