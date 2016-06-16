#-------------------------------------------------------------------------------
# Name:        Recombobulator
# Purpose:      Links processed image data back with the BUMP data
#
# Author:      clay
#
# Created:     19/03/2015
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

# * adds B1 and B7
# filter out site B, all nd/0 results for bands, and missing data from bump (type str)

import os
from os import listdir
from os.path import isfile, join, isdir
import csv
from datetime import date, datetime, timedelta
import logging
import logging.config
import inspect

# get sub modules
import wqu
#import discover_files
#import write_csv_output
#import read_csv

# get variables from file
from variables import *


def main():
    """
    Reads the processed output, filters out bad data.
    Gets matching BUMP data. Makes final output.
    """
    log = logger.getChild(__name__)

    # Read Master list
    master_list = wqu.read_csv(os.path.join(working_dir, settings['Image List']))

    # Read Processed image data
    aggregated_list = wqu.read_csv(os.path.join(working_dir, extract_dir, settings['Agg Output']))
    # 0             1   2           3   4   5           6           7           8           9           10          11          12          13          14
    #folder name	OID	4104000800	X	Y	crc_LE7027	crc_LE70_1	crc_LE70_2	crc_LE70_3	crc_LE70_4	crc_LE70_5	trc_LE7027	water_crc_	not_cloud   SID

    # Aggragated_list is a row containing the band values, mask values, and SiteID.
    # Using a combination of xFolder Name (lake/date codes)x WBID? and SiteID we put all values together
    #   from a single sample window for each band, and keep that average if it passes
    #   the masks & null checks. Output is a single value per SiteID per band.

    # this is final result
    result = []
    mask_rejects = []
    nd_rejects = []
    threshold_rejects = []
    rejects = [nd_rejects, mask_rejects, threshold_rejects]

    # this is result of the analysis operations
    # initialize final? output headers, in the output list
# maybe should be in Vars? is any of this optional?
    merge_result = [['folder name', 'WBID', 'lake name', 'scene', 'scene date', 'site id', 'B1', 'B1%complete', 'B2', 'B2%', 'B3', \
        'B3%', 'B4', 'B4%', 'B5', 'B5%', 'B7', 'B7%', 'Corrected Chlorophyl A', 'Turbidity, Field', 'season']]

    ## initialize mask check values, 0 did not work
    # masks are Water (NDWI) and Not Cloud. Desired results are Water T and Not Cloud 1
    ##test_value = ['0.0', '0.0']

# working master list > processed folders is great for following through with the orignial list
#   but it'd be much faster to just pull data from master list that we need for
#   the Extracts...

    # Use folder name to select data for each scene out of processed data
# should change to VAR
    folder_list = [i[0] for i in master_list[1:]]
    for f in folder_list:
        log.debug('iterating folder {}'.format(f))
        data_selection = [i for i in aggregated_list if f in i]
        log.debug('folder data selection: {}'.format(data_selection))
        if data_selection:

            # itereate by scene and evaluate final values (- Exclude Site list readings)
#! replace with dynamic index selection
            site_list = set([i[-1] for i in data_selection if settings['Exclude Site'] not in i[-1]])
            log.debug('site_list (- Exclude Site list): {}'.format(site_list))
            line_seed = [i for i in master_list[1:] if f in i][0]
            # temporary stack to store results for further processing
            temp = []

            for s in site_list:
                log.debug('processing site #: {}'.format(s))

                # seed is Folder Name, Lake Name, Scene Name, and Scene Date
                line = []
                line.extend(line_seed)
                log.debug('line result initialized as: {}'.format(line))
                percent_not_null = 0

                # var to hold summable line data during eval and iteration
                sums = []

                # append site_id converting '_' back to '-'
                line.append(s.replace('_', '-'))
                site_selection = [i for i in data_selection if s in i]
                # there should be as many selected items in this list as there were selected as the dimension of the utc_window
                log.debug('site_selection: {}/{}'.format(len(site_selection), site_selection))
                for site in site_selection:
                    log.debug('eval: {}'.format(site))
    # wouldjn't this be a lot less work if these weren't text?
                # evaluate top down (cloud > water > null) by iterating s
    # replace with dynamic index selection
                    log.debug('mask checks: {} {} / {} {}'.format(site[12], site[13], site[12] == '1.0', site[13] == '1.0'))
                    # check that the masks say "ok"
                    if site[12] and site[13] == '1.0':
                        # check how many band values are null
    # this is another needed dynamic pull
                        reading_list = [5, 6, 7, 8, 9, 10] # aka [5:11]
                        null_check = [j for j in site[5:11] if j == '0.0']
                        log.debug('null check, etc: {} {} {}'.format(null_check, len(site[5:11]), len(null_check)))
                        # all null, do not process this line further
                        if len(null_check) is not 0:
                            log.debug('> setting aside since null')
                            nd_rejects.append(site)
                        # otherwise evaluate the % not null
                        else:
                            log.debug('> not all null, send to temp stack')
                            temp.append(site)
                    else:
                        log.debug('> mask test failed, send to mask_rejects')
                        mask_rejects.append(site)

                log.debug('temp stack: {}/{}'.format(len(temp), temp))
                #now pick through the good ones
                # if temp caught any:
                # read through temp, by ColList. and check each Band for completenesss.
                # make average after filitering out nulls. save compelte % for later writing.
                if temp:
                    for i in reading_list:
                        log.debug('band to read: {}'.format(i))
                        # need to pull from 'temp' by Column 'i' now
                        col = [j[i] for j in temp]
                        log.debug('col {} from temp: {}'.format(i, col))
                        # actually having not nulls is better segue to sum for average
                        # count the nulls
                        count = len(col)
                        # count the not nulls
                        not_nulls = [i for i in col if i != '0.0']
                        log.debug('len: {}, not_nulls count for this band: {}'.format(count, len(not_nulls)))
                        # calc the average
                        sums = [float(i) for i in not_nulls if i]
                        avg = sum(sums) / len(sums)
                        log.debug('avg = {}'.format(avg))
                        # calc the completion ratio
                        percent_not_null = len(sums)/ count
                        log.debug('% not null = {}'.format(percent_not_null))
            # this is where a threshold check would go
                        line.append(avg)
                        line.append(percent_not_null)
                    # send line result to 'result' only if we had something in 'temp'
                    merge_result.append(line)
                else:
                    log.debug('>> temp was devoid of cases to process {}'.format(temp))

        else:
            log.debug('folder {} had no output'.format(f))
    log.debug('master list and aggregated merged result: {}'.format(merge_result))

    # find the wq file
    wq_file = wqu.discover_files(settings['WQ_Path'])
    # open wq file
    #bump_list = read_csv(os.path.join(working_dir, settings['Output Dir'], settings['Bump Data']))
    wq_list = wqu.read_csv(os.path.join(settings['WQ_Path'], wq_file[0]))
    # ['id', 'Station ID', 'Station Description', 'Sample Date', 'Sample Time', 'Corrected Chlorophyl A',
    #'Turbidity, Field', 'Sampler Comment', 'Analyst Comment']
    #i think there's confusion about result/merge_result, which is borking this r/i[3&4] call
    # for each line in result compare to site & date in bump list, appending sample data
    ## add aditional header rows to result

    for (x, i) in enumerate(merge_result):
        log.debug('enumerating {}/{}'.format(x, i))
        # skip header
        if x > 0:
            r = i
            log.debug('seeded r {}'.format(r))
# dynamic select plz
            site = i[5]
            date = i[4]
            log.debug('check for {} & {} in wq file'.format(site, date))
            ## dates are in '02/25/2001' format
            #do = datetime.strptime(date, "%m/%d/%Y").date()
            do = datetime.datetime.strptime(date, "%Y/%m/%d").date()
            date_check = []
            date_check.append(do)
            date_check.extend([do - settings['date_allowance']])
            date_check.extend([do + settings['date_allowance']])
            log.debug('date objects for check {}'.format(date_check))
            # bump list has date as string, so put t7hese back to string
            acceptable_dates = [datetime.datetime.strftime(d, "%m/%d/%Y") for d in date_check]
            log.debug('acceptable string dates: {}'.format(acceptable_dates))

            # wq file reads this: ['1', '520700050060-01S', 'Chandler Lake', '10/4/2000', '915', '', '6', '', '']

            #t1 = [i for i in wq_list if site in i and datetime.strptime(i[3], "%m/%d/%Y").date() in date_check]
            #print t1

# dynamic sel
            selection = [i for i in wq_list if site in i and datetime.datetime.strptime(i[3], "%m/%d/%Y").date() in date_check]
            log.debug('selection of bump: {}'.format(selection)) # try for id + date range

            if selection:
                # each data seems to have its own row... assume there are no dupes here
                chlor = ''
                turb = ''
                for j in selection:
                    log.debug('checking {}'.format(j[5:7]))
                    if j[5]:
                        chlor = j[5]
                    if j[6]:
                        turb = j[6]
                r.extend([chlor, turb])

    # check date to determin (simple) season
                if do.month > 9:
                    season = 'fall'
                elif do.month > 6:
                    season = 'summer'
                elif do.month > 3:
                    season = 'spring'
                elif do.month > 0:
                    season = 'winter'
                else:
                    season = 'error'
                log.debug('month = season/ {} = {}'.format(do.month, season))

                r.append(season)
                log.debug('keeping line {}'.format(r))
                result.append(r)

            else:
                # major problem, seeing how scenes were chosen based off the bump list
                # just drop this line from output then
                log.warning('deleting due to failing to match processed scene to a \
                    bump sample date:{}'.format(r))

        else:
            log.debug('seed result with header: {}'.format(i))
            result.append(i)


    # write output into output folder
    wqu.write_csv_output(result, settings['Extract Path'], settings['Final Output'])

    log.debug('result {}'.format(result))
    log.debug('reject summary follows:')
    for i in rejects:
        log.info('number of rejects: {}'.format(len(i)))
        log.info('list of rejects: {}'.format(i))

    log.info(u'fin')

if __name__ == '__main__':
    # setup logger
    log_ini = os.path.abspath( os.path.join(working_dir, script_dir, settings['Log Config']))
    logging.config.fileConfig(log_ini, defaults={'logfilename': settings['Log Name']})
    logger = logging.getLogger(os.path.basename(__file__).split('.')[0])

    main()