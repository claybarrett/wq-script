#-------------------------------------------------------------------------------
# Name:        ImageReadAndClip
# Purpose:      read a list of images, generate boxes from that list
#                   and read metadata
#
# Author:      clay
#
# Created:     19/01/2016
# Copyright:   (c) clay barrett 2016
# Licence:     see License.txt
#-------------------------------------------------------------------------------

# LS8 seems to already be TOA Refectance?
# also LS8 bands... are different UGH
# aaaand DNmin just goes with the last checked value if none meets the critera...

#   Open <some list> in <some directory> to read in images to process
#       Check that a matching clip (by code) is found in <clip dir>
#       Open each image
#           - read for DNmin, updated DNmin list
#           - subset each image
#           - copy each to new dir along with metadata

import os
import sys
from os import listdir
from os.path import isfile, join, isdir
import fileinput
import csv
import inspect
import logging
import logging.config
import pickle
import traceback

# import system modules
try:
    import numpy as np
except ImportError:
    print "You need numpy installed"
    sys.exit ( -1 )

# get variables from file
from variables import *

#import arcpy after vars, to get one setting
try:
    import arcpy
except ImportError:
    print "You need arcpy installed"
    sys.exit ( -1 )
from arcpy import env
env.overwriteOutput = settings['Arc Overwrite']

import wqu
# get sub modules
#from wqu import read_csv
#from wqu import list_find
#from wqu import get_metadata_name
#from wqu import process_metadata

def main():
    logger = main_logger.getChild(__name__)
    logger.debug('Input file: {}'.format(settings['Image List']))

    # read master list
    input_file = settings['Image List']
    input_path = os.path.join(working_dir, input_file)
    input_list = []
    clip_list = []
    # first check if it is present
    if os.path.isfile(input_path):
        input_list = wqu.read_csv(input_path)

    # generate box/clips and sample point files
        # get list of box codes from the input
        box_list = wqu.list_find(input_list, settings['Code Header'])[1]
        logger.debug('full code len & list: {} {}'.format(len(box_list), box_list))

        # reduce codes to just first half (scene code) and make uniq
        box_list = set([i.partition('-')[0] for i in box_list])
        logger.debug('uniq code & scene list: {} {}'.format(len(box_list), box_list))

        # check each is present in the box dir
        logger.debug('box_path: {}'.format(settings['Box Path']))
        # make sure the box dir exists, if not, make
        if not os.path.exists(settings['Box Path']):
            os.makedirs(settings['Box Path'])

        # get just the prefix from box file (lake id), only for SHPs
        boxes_in_dir = [i.partition('_')[0] for i in listdir(settings['Box Path']) \
                    if i.partition('_')[2] == settings['Box Suffix'][1:]] # - the _
        logger.debug('boxes_in_dir len & list: {} {}'.format(len(boxes_in_dir), boxes_in_dir))

## could check if all first? then make with the missing ones?
        # see if all of box_list can be found in boxes_in_dir list
        boxes_missed = [i for i in box_list if i not in boxes_in_dir]
        logger.debug('boxes_missed: {}'.format(boxes_missed))

### above, list comp this...
##        boxes_missed = []
##        for i in box_list:
##            if i in boxes_in_dir:
##                logger.debug('box {} found already created'.format(i))
##            else:
##                logger.warning('box {} NOT found already created'.format(i))
##                boxes_missed.append(i)

        if boxes_missed:
            logger.warning('boxes NOT all found already created: {}'.format(boxes_missed))
            # get shapefile path
            shape_filepath = [f for f in listdir(settings['Lake Shape Path']) \
                    if isfile(join(settings['Lake Shape Path'],f)) and os.path.splitext(f)[1] == '.shp']
            # warn user if input is missing
            if not shape_filepath:
                logger.warning('Missing Input from Lake Shape Path: {}'.format(shape_filepath))

            in_feature = os.path.join(settings['Lake Shape Path'], shape_filepath[0])
            logger.debug('in feature path: {}'.format(in_feature))

            for lake_id in boxes_missed:
                # check if you get anything before running the Select and Box
                sql_exp = """{0} = '{1}'""".format(arcpy.AddFieldDelimiters(in_feature, lake_headers['lake id']), lake_id)
                logger.debug('iterating box for sql_exp: {}'.format(sql_exp))

                # go ahead and make them

                # this var/output gets recycled since overwrite=true
                temp_layer = os.path.join(settings['Box Path'], "lyr_temp.shp")

                out_feature_class = os.path.join(settings['Box Path'], lake_id + settings['Box Suffix'])
                logger.debug('clipping to make {}'.format(out_feature_class))
                # Select the lake and Make the clip outline
                arcpy.Select_analysis(in_feature, temp_layer, sql_exp)
                result = arcpy.MinimumBoundingGeometry_management(temp_layer,  out_feature_class, "RECTANGLE_BY_AREA")

                if result.status == 4:
                    logger.debug('boxed scene {} as {}'.format(lake_id, out_feature_class))
                else:
                    logger.warning('failed to clip: {}'.format(lake_id))
        else:
            logger.debug('*** boxes all found already created')

        # check each is present in the box dir
        logger.debug('point_path: {}'.format(settings['Point Path']))
        # make sure the box dir exists, if not, make
        if not os.path.exists(settings['Point Path']):
            os.makedirs(settings['Point Path'])

        # Check for/make coordinate point files
        # get just the prefix, only for SHP files
        points_in_dir = [i.partition('_')[0] for i in listdir(settings['Point Path']) \
                        if i.partition('_')[2][0:] == settings['Point Suffix'][1:]]
        logger.debug('points_in_dir 1 len & list: {} {}'.format(len(points_in_dir), points_in_dir))

        # see if all of box_list can be found in points_in_dir list
        points_missed = [i for i in box_list if i not in points_in_dir]
        logger.debug('points_missed: {}'.format(points_missed))

        if points_missed:
            logger.warning('points NOT all found already created: {}'.format(points_missed))
            # get shapefile path
            shape_filepath = [f for f in listdir(settings['Lake Shape Path']) \
                    if isfile(join(settings['Lake Shape Path'],f)) and os.path.splitext(f)[1] == '.shp']
            # warn user if input is missing
            if not shape_filepath:
                logger.warning('Missing Input from Lake Shape Path: {}'.format(shape_filepath))

            in_feature = os.path.join(settings['Lake Shape Path'], shape_filepath[0])
            logger.debug('shape filepath: {}'.format(in_feature))

            for lake_id in points_missed:
                # get coord file path
                coord_filepath = [f for f in listdir(settings['Coord Path']) \
                        if isfile(join(settings['Coord Path'],f)) and os.path.splitext(f)[1] == '.shp']
                in_feature = os.path.join(settings['Coord Path'], coord_filepath[0])
                logger.debug('coord filepath: {}'.format(in_feature))
                temp_layer2 = os.path.join(settings['Point Path'], "pnt_temp.shp")
                out_points = os.path.join(settings['Point Path'], lake_id + settings['Point Suffix'])
                logger.debug('clipping to make pointfile: {}'.format(out_points))
                sql_exp2 = """{0} = '{1}'""".format(arcpy.AddFieldDelimiters(in_feature, coord_headers['lake id']), lake_id)
                logger.debug('with sql_exp2: {}'.format(sql_exp2))

                # Select the lake and Make the clip outline
                arcpy.Select_analysis(in_feature, temp_layer2, sql_exp2)
                #result = arcpy.MinimumBoundingGeometry_management(temp_layer,  out_feature_class, "RECTANGLE_BY_AREA")
                arcpy.CopyFeatures_management(temp_layer2, out_points)

# should del pnt_temp.shp as cleanup?
        else:
            logger.debug('*** points all found already created')

        # now that Boxes and Point files are made, clean up any empty dirs from this process
        #   to prevent false starts in later processes
        folders_in_box_dir = [i for i in listdir(settings['Box Path']) if isdir(os.path.join(settings['Box Path'], i))]
        for dir in folders_in_box_dir:
            try:
                os.rmdir(dir)
            except OSError as ex:
                if ex.errno == errno.ENOTEMPTY:
                    logger.debug('directory {} not empty'.format(dir))

        # get list of scenes
        scene_list = wqu.list_find(input_list, settings['Scene Header'])[1]
        logger.debug('scene len & list: {} {}'.format(len(scene_list), scene_list))
        scene_list = set(scene_list)
        logger.debug('uniq len & scene list: {} {}'.format(len(scene_list), scene_list))

        # now iterate through the input list, sorted by scene, and make clips of the LS images
        logger.debug('ls scene dir: {}'.format(settings['Scene Path']))

        # Start with each scene in the input_list
        for scene_id in scene_list:
            logger.debug('processing scene: {}'.format(scene_id))
            parameter_dict = {}
        # read metadata to store in dict
            metadata_file = wqu.get_metadata_name(os.path.join(settings['Scene Path'], scene_id))
            ##logger.debug('metadata name: {}'.format(metadata_file))
        # no metadata file = no processing/clipping/etc
            if os.path.isfile(metadata_file):
                #(gain, bias, lmax, lmin, qc_lmax, qc_lmin) = process_metadata(metadata_file)
                #parameters = process_metadata(metadata_file, x)
                parameter_dict, scene_vars = wqu.process_metadata(metadata_file, scene_id)
                logger.debug('metadata sneak peak: {}'.format(parameter_dict))
                logger.debug('scene metadata: {}'.format(scene_vars))


                # Get all the scenes
                scenes = [i for i in listdir(settings['Scene Path']) if scene_id in i and i.partition('.')[2] == 'TIF' ]
                logger.debug('scenes selected from scene dir 1: {}'.format(scenes))

                # drop bands not on in the Pick List
                scenes = [i for i in scenes if i.partition('_')[2][1] \
                            in settings['Pick Bands']]
                logger.debug('reduced scenes selected from scene dir : {}'.format(scenes))

    # NO metadata already read. this is where i'd extract the zipped raw files if I were so inclined
# and this would be where to make a DNmin func
                # collect each scene's DNmin
                dnmin = {}
                for i in scenes:
                    scene = os.path.join(settings['Scene Path'], i)
                    logger.debug('scene path: {}'.format(scene))
                    img = arcpy.Raster(scene)
                    # take off extention to match what the metadata keys look like
                    this_band = i.partition('_')[2].split('.')[0]

                    # check if RAT present or needs built
                    test = img.hasRAT
                    if test:
                        logger.debug('{} has RAT'.format(i))
                    else:
                        logger.debug('{} has no RAT'.format(i))
                        t = arcpy.BuildRasterAttributeTable_management(scene, "NONE")
                        if t:
                            logger.debug('{} had RAT built'.format(i))
                        else:
                            logger.debug('{} failed to build RAT'.format(i))

                    if test or t:
                        # these should be acending from lowest value
                        #with arcpy.SearchCursor(scene,"","","Value;Count","") as rows:
                        with arcpy.da.SearchCursor(scene, ["VALUE", "COUNT"]) as rows:
                            for row in rows:
                                d = 0
                                #val = row.getValue("VALUE")
                                val = row[0]
                                #count = row.getValue("COUNT")
                                count = row[1]
                                #logger.debug('evaluate: {}/{} {}/{}'.format(val, type(val), count, type(count)))
# import from future or code subsitute for numpy's "lowest # of count" function
#   then the RAT stuff could go too
                                # find the lowest value with a count of at least 100
                                if int(val) > 0 and int(count) > 100:
                                    d = int(val)
                                    dnmin[this_band] = d
                                    logger.debug('{} is DNmin'.format(d))
                                    break
                               # if dnmin:
                               #     DN_list.append([i, dnmin])
                               #     logger.debug('{} is DNmin'.format(dnmin))
                               #     break
                                else:
                                    logger.debug('{}({}) is not DNmin'.format(val, count))

                logger.debug('DNmin: {}'.format(dnmin))
                # add dnmin to parameter dict
                parameter_dict['dnmin'] = dnmin

                logger.debug('parameters as dict: {}'.format(parameter_dict))

                # store it by scene name
                Correction_Parameters[scene_id] = parameter_dict
                Correction_Parameters[scene_id]['sat_id'] = scene_vars[0]
                Correction_Parameters[scene_id]['julian_date'] = scene_vars[1]
                Correction_Parameters[scene_id]['sun_elevation'] = scene_vars[2]
                #Correction_Parameters[x] = [i for n in p for n in parameters]
                logger.debug('assembed correction parameters for {}: {}'.format(scene_id, Correction_Parameters))

                # Get all the lakes that need clipped out of this image
                process_list = [i for i in input_list if scene_id in i]
                # Make sure you have something:
                if process_list:
                    logger.debug('lakes to be clipped: {}'.format(process_list))

                    # Process this list
                    for i in process_list:
# actually need some way to keep from making empty dirs if clip fails...
                        # make sure the output dir exists, if not, make
                        out = os.path.join(working_dir, settings['Clip Path'], i[0])
                        if not os.path.exists(out):
                            os.makedirs(out)
                            logger.debug('Created clip output dir: {}'.format(out))

                        # Set box/MBG variables
                        rectangle = '#'
                        box_feature = settings['Box Path'] + '/' + i[1] + settings['Box Suffix']
                        nodata_value = '0'
                        clipping_geometry = 'ClippingGeometry'
                        maintain_clipping_extent = 'NO_MAINTAIN_EXTENT'

                        # check for the box file before clipping images
                        if os.path.exists(box_feature):
                            # check if the box file is empty
                            if arcpy.management.GetCount(box_feature)[0] > '0':
                                #print 'box debug:', box_feature, type(box_feature), arcpy.Describe(box_feature)

                                # Clip and output all bands of the scene
                                for j in scenes:
                                    logger.debug('prep for clipping: {}'.format(j))
                                    in_raster = os.path.join(settings['Scene Path'], j)
                                    logger.debug('input: {}'.format(in_raster))
                                    #out_raster = os.path.join(working_dir, output_dir, 'c_'+j)
                                    out_raster = os.path.join(settings['Clip Path'], i[0], 'c_'+j)
                                    logger.debug('output: {}'.format(out_raster))

                                    try:
                                        result = arcpy.Clip_management(in_raster, rectangle, out_raster,
                                            box_feature, nodata_value, clipping_geometry, maintain_clipping_extent)

                                    except Exception, e:
                                        # If an error occurred, print line number and error message

                                        tb = sys.exc_info()[2]
                                        #print "Line %i" % tb.tb_lineno
                                        #print e.message
                                        logger.warning('Line {}: {}'.format(tb.tb_lineno, e.message))

                            else:
                                logger.warning('box was empty: {}'.format(j))
                        else:
                            logger.warning('lack of box file {} for clipping {}'.format(box_feature, i))
                else:
                    logger.warning('process_list couldnt find: {}'.format(scene_id))

            else:
                logger.critical('metadata not found!: {}'.format(metadata_file))
    # what happens on the next step if there's no MD?

        # write Correction Parameters dictionary to Output's dir
        logger.debug('writing corr parameters to {}'.format(settings['MD file']))
        # make sure the Output_Dir exists, or make
        if not os.path.exists(settings['Clip Path']):
            os.makedirs(settings['Clip Path'])
        # and pickle it for next script to use w/o reading the file we just wrote
        with open(os.path.join(settings['Clip Path'], settings['MD file']), 'wb') as handle:
            pickle.dump(Correction_Parameters, handle)

    else:
        logger.warning(u'Missing the input file \'Image List\': {}'.format(settings['Image List']))

    logger.debug('finished!')


if __name__ == '__main__':
    # make sure the log dir exists, if not, make
    if not os.path.exists(settings['Log Path']):
        os.makedirs(settings['Log Path'])

    # setup logger
    log_ini = os.path.abspath( os.path.join(working_dir, script_dir, settings['Log Config']))
    logging.config.fileConfig(log_ini, defaults={'logfilename': os.path.join(settings['Log Path'], settings['Log Name'])})
    main_logger = logging.getLogger(os.path.basename(__file__).split('.')[0])
    main()