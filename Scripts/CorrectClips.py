#-------------------------------------------------------------------------------
# Name:        Correct&COSTDOSImage Clips
# Purpose:
#
# Author:      clay
#
# Created:     19/jan/2016
# Copyright:   (c) clay barrett 2016
# Licence:     see License.txt
#-------------------------------------------------------------------------------
# would like to split out cost and dos portions of correction

# Reads metadata dict from ImageReadAndClip
# Reads in solar distance file (d.xcl)
# Iterate through CLIP dir
#   check for scenes
#   check if scenes are all NoData
#   convert to radiance
#       unless it's the Thermal band, which goes to Kelvins
#   *convert DNmin to radiance
#   *do COST-DOS
#   create NDWI mask
#   create No Cloud mask
#   Select values around sample points
# Aggregate results of select values

import os
from os import listdir
from os.path import isfile, join, isdir
import sys
import csv
import numpy as np
import math
import inspect
import logging
import logging.config
import pickle
from itertools import islice

# get sub modules
import wqu

# get variables from file
from variables import *

#import arcpy
try:
    import arcpy
except ImportError:
    print "You need arcpy installed"
    sys.exit ( -1 )
from arcpy import env
env.overwriteOutput = settings['Arc Overwrite']

from arcpy.sa import *
#Check out the Spatial Analyst extension
try:
    arcpy.CheckOutExtension("spatial")
except ImportError:
    print "You need access to Spatial Analyst"
    sys.exit ( -1 )

def main():
    logger = main_logger.getChild(__name__)

    # open the metadata dict
    with open(os.path.join(settings['Clip Path'], settings['MD file']), 'rb') as handle:
        Correction_Parameters = pickle.loads(handle.read())
        logger.debug('read dict: {}'.format(Correction_Parameters))

    # open the solar distance file, read into a dict
    with open(os.path.join(working_dir, settings['SD File']), 'rb') as handle:
        reader = csv.reader(handle)
        for row in islice(reader, 1, None): #skip header row
            sun_d[row[0]] = float(row[1])
        #logger.debug('sun_d: {}'.format(sun_d))

    # read the folders to process in the Clip folder
    logger.debug('clip output dir is: {}'.format(settings['Clip Path']))

    folders_in_dir = [i for i in listdir(settings['Clip Path']) if isdir(os.path.join(settings['Clip Path'], i))]
    logger.debug('folders_in_dir len & list: {} {}'.format(len(folders_in_dir), folders_in_dir))

    # cycle thorugh contents of Process folder
    for folder in folders_in_dir:
        folder_path = os.path.join(working_dir, settings['Clip Path'], folder)
        logger.debug('iterating folder: {}'.format(folder_path))
        # get list of scenes with clip prefix and tif ext
        scenes_in_dir = [i for i in listdir(folder_path) if i[:2] == \
            settings['Clip Prefix'] and i.partition('.')[2] == settings['LS Extention']]
        logger.debug('scenes_in_dir {} len & list: {} {}'.format(folder, \
            len(scenes_in_dir), scenes_in_dir))

        # skip this folder if no scenes found
        if len(scenes_in_dir) > 0:
            scene_key = set([i.split('_')[1] for i in scenes_in_dir])
            logger.debug('scene_key {}'.format(scene_key))

            # load a subset of Corr Para for this scene
            scene_d = dict((i, Correction_Parameters[i])
                for i in Correction_Parameters if i in scene_key)
            # strip dict down to just parameters, no need for scene_id now
            scene_d = scene_d.values()[0]
            logger.debug('scene parameters {}'.format(scene_d))

            for scene in scenes_in_dir:
                scene_path = os.path.join(folder_path, scene)
                band = scene.split('_')[2].split('.')[0]
                logger.debug('reading {}, band = {}'.format(scene_path, band))

                # check if the image contains any data before trying to process
                radiance_ras = Raster(scene_path)
                #   this returns a result object... part of which is string of
                #       result that needs converted to binary to be useful
                nd_check = arcpy.GetRasterProperties_management (radiance_ras, 'ALLNODATA')
                logger.debug('nd_check = {}'.format(nd_check))
                t = bool(int(nd_check.getOutput(0)))

                if t:
                    logger.warning('image {} is all NoData, no processing'.format(scene))
                else:
# so many vars, could just send the rescale factor?
                    # just convert to radiance
                    #radianceRaster = calcRadiance(LMAX, LMIN, QCALMAX, QCALMIN, BANDFILE, outfolder)
                   # (LMAX, LMIN, QCALMAX, QCALMIN, QCAL, outfolder)
                    radiance_ras = wqu.calc_radiance(scene_d['lmax'][band], scene_d['lmin'][band], \
                        scene_d['qc_lmax'][band], scene_d['qc_lmin'][band], scene_path , folder_path)

                    # B6 > Temp, everything else goes for Correction
                    if band == 'B6':
                        logger.debug('Convert B6 to temp')
                        tc_test = wqu.b6_to_temp(scene_d['sat_id'], radiance_ras)
                        if tc_test:
                            logger.debug('{} successfully converted to kelvins'.format(scene_path))
                        else:
                            logger.debug('{} failed to convert to kelvins'.format(scene_path))
                    else:
                        # DNMin is in DN. Has to be converted to Radiance seperatley form scene.
                        # COST DOS correction (includes scaling DNMin)
                        dnmin_as_radiance = 0.0
# can divide COST from DOS?
                        if settings['Do_COSTDOS']:
                            # convert the stored DNmin value to radiance

                            # example
                            #offset = (LMAX - LMIN)/(QCALMAX-QCALMIN)
                            #dnmin_as_radiance = (offset * (dnmin - QCALMIN)) + LMIN
            # yes move this up to above Calc_rad, but be sure to send other stuff it needs
                            offset = (scene_d['lmax'][band] - scene_d['lmin'][band])/(scene_d['qc_lmax'][band] - scene_d['qc_lmin'][band])
                            #HLmin = (offset * ((scene_d['dnmin'][band] - scene_d['qc_lmin'][band])) + scene_d['lmin'][band])
                            HLmin = (scene_d['qc_lmin'][band] + scene_d['dnmin'][band] * offset)
                            logger.debug('HLmin: {}'.format(HLmin))
                            #scale_factor = scene_d['lmin'][band] + (scene_d['dnmin'][band] *(scene_d['lmax'][band] - scene_d['lmin'][band])/ \
                            #    (scene_d['qc_lmax'][band] - scene_d['qc_lmin'][band]))

                            # now correct it
                            solarZenith = (90.0 - float(solarElevation))* (math.pi / 180)    #Converted from degrees to radians
                            #solarZenith = math.pow(((90.0 - float(scene_d['sun_elevation'])) * (math.pi / 180)), 2)    #Converted from degrees to radians
                            logger.debug('solar zenith in rad: {}'.format(solarZenith))
                            ##solarZenith = (((90.0 - (float(scene_d['sun_elevation'])))*math.pi)/180) #Converted from degrees to radians
            #(math.pi * (radiance - dnmin) * math.pow(solarDist, 2)) / (ESUN * math.cos(solarZenith))
                            logger.debug('n = {}'.format((0.01 + wqu.get_ESUN(band, scene_d['sat_id']) * math.cos(solarZenith))))
                            logger.debug('d = {}'.format((math.pow(sun_d[scene_d['julian_date'].lstrip('0')], 2) * math.pi)))
                            ##L1percent = ((0.01 + (get_ESUN(band, scene_d['sat_id']) * math.cos(solarZenith))) / (math.pow(sun_d[scene_d['julian_date'].lstrip('0')], 2)))
                            L1percent = ((0.01 + wqu.get_ESUN(band, scene_d['sat_id']) * math.cos(solarZenith)) / \
                                (math.pow(sun_d[scene_d['julian_date'].lstrip('0')], 2) * math.pi))
                            logger.debug('L1percent: {}'.format(L1percent))
                            dnmin_as_radiance = HLmin - L1percent
                            logger.debug('DNMin of {} converted to radiance {}'.format(scene_d['dnmin'][band], dnmin_as_radiance))
                        else:
                            logger.debug('DNMin is 0 (preset)')
                            #scale_factor = ((scene_d['lmax'][band] - scene_d['lmin'][band])/ \
                            #    (scene_d['qc_lmax'][band] - scene_d['qc_lmin'][band]))
                        #reflectanceRaster = calc_reflectance(solarDist, ESUNVAL, solarElevation, radianceRaster, Lhaze, outfolder)
                        reflectanceRaster = wqu.calc_reflectance(sun_d[scene_d['julian_date'].lstrip('0')], \
                            wqu.get_ESUN(band, scene_d['sat_id']), scene_d['sun_elevation'], radiance_ras, dnmin_as_radiance, folder_path)

        # get_ESUN(bandNum, SIType)
        #http://cegis.usgs.gov/soil_moisture/pdf/A%20Straight%20Forward%20guide%20for%20Processing%20Radiance%20and%20Reflectance_V_24Jul12.pdf
        # steve left some important stuff out
        #SUN_ELEVATION= 65.49;
        #The value (65.49) is the Elevation angle in degrees (values can range between <= -90.0 to
        #<= 90.0).

            # now from the corrected scenes grab band 2 & 5 (if any)
            corrected_prefix = settings['Corr Prefix'] + settings['Rad Prefix'] + settings['Clip Prefix']
            corrected_scenes = [i for i in listdir(folder_path) if corrected_prefix in i \
                and i.partition('.')[2] == settings['LS Extention']]
            logger.debug('corrected scene list 2: {}'.format(corrected_scenes))
         # need to get TRC in there for cloud_mask

            if len(corrected_scenes) > 2:
# by band processing will not work for LS8
                B2 = [i for i in corrected_scenes if settings['Green Band'] in i][0]
                B5 = [i for i in corrected_scenes if settings['SWIR Band'] in i][0]

                # send to ndwi
                if B2 and B5:
                    ndwi = wqu.ndwi_mask(folder_path, B2, B5)
                else:
                    logger.warning('ndwi failed to process: {} {}'.format(B2, B5))

                B3 = [i for i in corrected_scenes if settings['Red Band'] in i][0]
                B4 = [i for i in corrected_scenes if settings['NIR Band'] in i][0]
# move to VARS
                unwanted_thermal = 'VCID_2'
                thermal_prefix = settings['Temp Prefix'] + settings['Rad Prefix'] + settings['Clip Prefix']
                logger.debug('thermal_prefix: {}'.format(thermal_prefix))
    # apparently we can Null Out a thermal band, and still get here...
        # so handle it gracefully here at least
                try:
                    B6 = [i for i in listdir(folder_path) if thermal_prefix in i \
                        and i.partition('.')[2] == settings['LS Extention'] and unwanted_thermal not in i][0]
                except:
                    logger.warning('appears the thermal band nulled out,\
                    cannot make cloud mask')
                else:
                    corrected_scenes.append(B6)

                    # do cloud mask
                    cloud = wqu.not_cloud_mask(folder_path, [B2, B3, B4, B5, B6])
                    # Finally call select, only after all the masks successfully fire
                    wqu.select_values(folder_path, corrected_scenes, cloud)
                    # which searches out the masks from before to exlude from extraction
            else:
                logger.debug('No corrected scenes to process: {}'.format(corrected_scenes))
        else:
            logger.debug('No scenes found to process: {}'.format(scenes_in_dir))

    # Call aggregator to collect all the extracted values in a single output
    wqu.aggregator(1)


    logger.debug(u'finished')

if __name__ == '__main__':
    # setup logger
    log_ini = os.path.abspath( os.path.join(working_dir, script_dir, settings['Log Config']))
    logging.config.fileConfig(log_ini, defaults={'logfilename': settings['Log Name']})
    main_logger = logging.getLogger(os.path.basename(__file__).split('.')[0])
    main()