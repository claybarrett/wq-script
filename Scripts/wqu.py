#-------------------------------------------------------------------------------
# Name:        wq script utility functions
# Purpose:
#
# Author:      clay barrett
#
# Created:     14/05/2016
# Copyright:   (c) admin 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from itertools import chain

def flatten(l):
    """Turns any depth of list into a flat list.
    Args:
        l (list): list you want to flatten
    Returns:
        result (list): flattened list
    """

    result = []
    #chain = itertools.chain.from_iterable(l)
    result = list(chain(*l))

    return result

import logging
import inspect
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

def list_find(list, search_term, header=0):
    """
    Find if something is found in a list of lists, and return that column's data
    Assume header is false, since returning that was added later
what does header mean?
    Returns ['R, C', 'List of stuff -header']
    """
    logger = module_logger.getChild(__name__)
    #log = logger.getChild(inspect.currentframe().f_code.co_name)
    #logger = log.main(__file__, settings['Log Path'])
    logger.debug('Initializing {}'.format(__file__))
    logger.debug('for {} in {}'.format(search_term, list))
    result = []

    # see if term is in the list
    term_row = [idx for idx in list if search_term in idx]
    logger.debug('term row: {}'.format(term_row))
    # possible outcomes are 0 = not found, 1 = expected to find one time, >1 = problem...
    # this is a list comp, so result is a list. check length!
    if len(term_row) == 1:
        # get the index of the row term was found in
        term_row_i = [i for i, x in enumerate(list) if search_term in x][0]
        logger.debug('term row_i: {}'.format(term_row_i))
        # get the column index it was found in
        term_col_i = [x for x, i in enumerate(term_row[0]) if i == search_term][0]  # returns i as list, so [0]
        logger.debug('term col_i: {}'.format(term_col_i))
        # with start row and col found, find last col and return a selection
        # only get from header row +1 to last row
        # can deal with an extra, blank line result on return
        selection = [x[term_col_i] for idx, x in enumerate(list) if idx in range(term_row_i +1, len(list))] # but this leaves blank on the end of names
        logger.debug('list_find selection dump: {}'.format(selection)) # should gaurd against blanks above header row

        # stick header on if desired
        if header:
            selection.insert(search_term, 0)
            [[term_row_i, term_col_i], selection]
        else:
            result = [[term_row_i, term_col_i], selection]
    else:
        logger.warning(u'Failed to find {} exactly once in list'.format(search_term))
        result = [0, 0]
    logger.debug('list_find result: {}'.format(result))
    return result

import os
import logging
import inspect
from datetime import date, datetime, timedelta
import variables
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])


def objectify_dates(args, format):
    """
    Takes a date string(s) in the forms both DD/MM/YYYY YYYY/MM/DD, returns a date object(s).
    Temp variance to just pass through Date header
    """
    log = module_logger.getChild(__file__)
    log.debug('Initializing {}'.format(__file__))
    #log.debug('with: {}'.format(args))
    result = []

    for i in args:
        date_string = i.split("/")
        #logger.debug('split date: {}'.format(date_string))
        # date(year, month, day) --> date object
        if format == 0:
            date_obj = date(int(date_string[2]), int(date_string[0]), int(date_string[1]))
        elif format == 1:
            date_obj = date(int(date_string[0]), int(date_string[1]), int(date_string[2]))
        else:
            logger.warning('undefined format!')
        #logger.debug('date object from split date: {}'.format(date_obj))
        result.append(date_obj)
    return result

from os import listdir
from os.path import isfile, join, isdir
import logging
import inspect
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])


def discover_files(loc):
    """
    Search curdir for a csv or txt in the Raw Input dir.
    This is the list of locations and dates which are used to select images from
    the other directory full of LS metadata "Target List"
Lets use a working dir, and then a file name "Test list.xls". load the xlreader.
Get the list using parse?
Read check for clip, read for dnmin, then clip and save to new dir.
    """

    log = module_logger.getChild(__name__)
    #logger = log.main(__file__, settings['Log Path'])
    #log = logging.getLogger('addon')
    log.info(u'Initializing {}'.format(__file__))
    result = []

    # Load all Input_Types files in curdir to a list
    file_list = [ f for f in listdir(loc) if isfile(join(loc,f)) \
    and os.path.splitext(f)[1] in settings['Input_Types']]
    log.debug('found anything?: {}'.format(file_list))

    # See if it's a CSV or TXT file
    for file in file_list:
        extention =  os.path.splitext(file)[1]
        log.debug('checking extention: {}'.format(extention))
        # See if it's a csv
        if extention.lower() == '.csv':
            log.debug('found a csv file: {}'.format(file))
            result.append(join(loc,file))
        elif extention.lower() == '.txt':
            log.debug('discovered a txt file: {}'.format(file))
            result.append(join(loc,file))
        elif extention.lower() == '.shp':
            log.debug('discovered a shp file: {}'.format(file))
            result.append(join(loc,file))
    	# Cannot handle multiple files, but need to catch the error
            #result.append(file)
                # Good enough!
            #result = file
        else:
            log.debug('no {}  file was found'.format(settings['Input_Types']))
    if result:
        log.debug('found an input')
        ## uh raise exception

    return result

import logging
import inspect
import csv
from variables import *
from operator import itemgetter
import arcpy

module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

def read_file_selection(*args):
    """
    Find selected indices, then return result of only that from a shp.
    Unique values only!

    Returning list of tuples [(head 1, head 2), (d1, d2), (etc)]
    """
    log = module_logger.getChild(__name__)
    #log = logging.getLogger(inspect.currentframe().f_code.co_name)
    #logger = log.main(__file__, settings['Log Path'])
    log.info(u'Initializing {}'.format(__file__))
    log.debug('with: {}'.format(args))

    input = args[0]
    headers = args[1]
    get_indices = []    # the index for headers we wish to collect
    result = []

    # sort through cases, set input for reading (next)
    for i in input:
        # open for txt and csv files:
        if input[0].partition('.')[2] in ['csv', 'txt']:
            log.debug('setting up reader')
            with open(i, 'r') as csvfile:
                reader = csv.reader(csvfile)
                #read_data = reader
# list select? for each thing
#then read using those Indices
# else: failed to find {{}
                for (j, row) in enumerate(reader):
                    # if it's first row, get indices
                    if j == 0:
                        log.debug('header: {}'.format(row))
                        log.debug('seeking header values: {}'.format(headers))
                        # just assume headers are present, unique
                        for v in headers:
                            get_indices.append(row.index(v))
                        log.debug('found indices: {}'.format(get_indices))


                        # then get those header values into result
                        #sel = [i[g] for i in row for g in get_indices]
                        # no sel = [i[get_indices] for i in row]
                        sel = list(itemgetter(*get_indices)(row))
                        log.debug('sel: {}'.format(sel))
                        result.append(sel)
                    # just grab using the found indices
                    else:
                        sel = itemgetter(*get_indices)(row)
                        log.debug('sel: {}'.format(sel))
                        # if it's already in result, do not append
                        if sel in result:
                            log.debug('sel was redundant')
                        else:
                            log.debug('appending sel')
                            result.append(list(sel))

        # open and read if shp:
        else:
            log.debug('setting up cursor')
            result.append(headers)
            #with arcpy.da.SearchCursor(i, headers) as cursor:
# need generator to change dict entries into the SQL statement... which I can't figure out
# "WBID" works, but not "WBID, SITE_ID"... any list is failing
            #sql_exp = arcpy.AddFieldDelimiters(i, headers)
            #input_fields = [f.name for f in arcpy.ListFields(i)]
            #log.debug('sql_exp: {}'.format(sql_exp))
            with arcpy.da.SearchCursor(i, headers) as cursor:
                for row in cursor:
                    result.append(list(row))
                    #print row
                    #test = [i.strip(' ') for i in row]
                    #if all(test):
                    #    log.debug('appending row: {}'.format(row))
                    #    result.append(list(row))
                    #else:
                    #    log.debug('discard row, had blank: {}'.format(row))

    log.debug('result: {}'.format(result))

    return result

from os import listdir
from os.path import isfile, join
import logging
import inspect
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

def select_md_files(*args):
    """
    input: settings['Metadata_Path'], unique_lake_id_list
    returns: [[id, [md files]], *]

    Get all file names from the metadata folder.
    """
    log = module_logger.getChild(__name__)
    #log = logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    result = []
    loc = args[0]
    selection_list = args[1]

    prob_list = []
# go use arg list passed to check we're "complete"

    # search MD dir for file names
    file_list = [f for f in listdir(loc) if isfile(join(loc, f)) \
         and os.path.splitext(f)[1] in settings['Input_Types']]
    log.debug('discovered metadata files: {}'.format(file_list))

    for i in selection_list:

        # checking there's a 45, 7a, and 7b md file for each lake
        l_result = []
        # key
        key = [0, 0, 0]
        log.debug('checking for {}'.format(i))
        # get just this lake name
        sub_list = [x for x in file_list if x.find(i.strip()) == 0]
        #sub_list = [x for x in file_list ]
        # check for all 3 LS archives. already tested as .csv, so can grab from back
        log.debug('found {}'.format(sub_list))

        # case for result or not
        if sub_list:
            log.debug('found {}'.format(sub_list))
##            for x in sub_list:
##                if x[-6:-4] == '45':
##                    key[0] = 1
##                elif x[-6:-4] == '7a':
##                    key[1] = 1
##                elif x[-6:-4] == '7b':
##                    key[2] = 1
##                else:
##                    log.warning('untyped case for : {}'.format(x))
##            log.debug('key {}'.format(key))
        else:
            Problem_List.append(['No match found for ' + str(i)])

        ## decide to keep or reject the result
        l_result = [i, sub_list]
        log.debug('one line result: {}'.format(l_result))
        result.append(l_result)
##        if all(key):
##            result.append(l_result)
##        else:
##            prob_list.append([i, sub_list, key])

    if Problem_List:
        log.warning('Partial found list:')
        for i in Problem_List:
            log.warning('{}'.format(i))

    # which lakes didn't get a match?
    if prob_list:
        log.warning('Missmatch list (OWRB list w/o data in dir):')
        for i in prob_list:
            log.warning('{}'.format(i))

    # any in dir which weren't on the owrb list? (subset from read file_list)
    missing_data_list = [x[:-6] for x in file_list if x in selection_list]
    if missing_data_list:
        log.warning('Data w/o an entry on OWRB list:')
        for i in set(missing_data_list):
            log.warning('{}'.format(i))

    log.debug('select_md_files result: {}'.format(result))
    return result

import logging
import inspect
import csv
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

#from wqu import objectify_dates

def parse_metadata(unique_lake_id_list, read_wq_file, id_md_list):
    """
    Read through the id_md_list, checking each metadata by name for the desired
    date range.
    Input: uniq list of lake ids, list of lake sample dates, list of all csv files (LS metadata)
    Returns ['Lake ID', 'Lake name', [Lake file list], [list of same date objects], [selected metadata]', *]
    """
    log = module_logger.getChild(__name__)
    #log = logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with: {} \n {} \n {}'.format(unique_lake_id_list, read_wq_file, id_md_list))
    result = []

    # cull lakes w/o all the data
    # by linking with ID now that it's here
    ## CANNOT since the inputs havn't been linked

    # create ID field for read_wq_file from the Site_ID field
    for (x, i) in enumerate(read_wq_file):
        if x == 0:
            i.insert(0, 'WBID')
        else:
            id = i[1].partition('-')[0]
            log.debug('wq file id: {}/{}'.format(i, id))
            i.insert(0, id)
    log.debug('sample id\'d wq file: {}'.format(read_wq_file[:3]))

    # make a name list
    #lake_name = []

    #lake_name.append('Lake Name')

    # iterate through the id_md_list files
    for i in sorted(id_md_list):
        lake_id = i[0]
        md_files = i[1]
        # get lake name from wq file
        lake_name = list(set([n[1] for n in read_wq_file if lake_id in n]))
        #lake_name = [n[1] for n in read_wq_file if lake_id in n]
        #lake_name = lake_name[0]
        log.debug('parsing: {}/{}'.format(lake_id, lake_name))

        log.debug('metadata files to read: {}'.format(md_files))
        # get index of date column from header
        date_index = [x for x, i in enumerate(read_wq_file[0]) if i == wq_headers['date_header']][0]
        log.debug('wq file\'s date_index: {}'.format(date_index))

        # get sample events dates from wq file
        dates = [i[date_index] for i in read_wq_file if lake_id in i]

        if dates:
            dates = list(set(dates))
            log.debug('collected dates: {}'.format(dates))
            #dates_objectified = objectify_dates(dates, 0)
            dates_objectified = objectify_dates(dates, 0)
            log.debug('date object list: {}'.format(dates_objectified))
        else:
            log.debug('failed to catch any dates: {}'.format(dates))

        # iterate through the md files and extract image dates
        f_result = []
        for file in md_files:
            log.debug('reading: {}'.format(file))
    ## slice to drop the completion key#[:2]
            with open(join(settings['Metadata_Path'], file), 'r') as csvfile:
                reader = csv.reader(csvfile)

                # pull the header and list_find to just read desired cols
                read_cols = []
                header = reader.next()
                for term in metadata_headers.values():
                        read_cols.append(header.index(term))
                log.debug('selected cols index(es): {}'.format(read_cols))

                # put those 2 results together ##then select by +/- 1 day from dates list.
                for (x, row) in enumerate(reader):
                    l_result = []
                    for i in read_cols:
                        l_result.append(row[i])
                    f_result.append(l_result)
                    log.debug('read in: {}'.format(l_result))
        log.debug('lake result: {}'.format([lake_id, lake_name, md_files, dates_objectified, f_result]))
        result.append([lake_id, lake_name, md_files, dates_objectified, f_result])
    return result

import logging
import inspect
from itertools import izip
import flatten
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

#from wqu import objectify_dates

def select_images(args):
    """
    Accept a list with scenes and dates. Choose just the days within the date_allowance.
    Accepting ['lake id, lake name, lake files, dates object list, [scenes & dates *]', *]
    Returns ['lake id, lake name, lake files, dates object list,
    [scenes, date_objects, * that are within date_allowance]', *]
    """
    log = module_logger.getChild(__name__)
    #log = logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    result = []
    scene_count = 0

    for lake in args:
        l_result = []
        log.debug('select images doing: {}'.format(lake))
        log.debug('reading: {}'.format(lake[:3]))
        # get lake name
        l_result.append(lake[0])
        l_result.append(lake[1])
        l_result.append(lake[2])
        l_result.append(lake[3])

        # check if there is any scenes
        if lake[4]:
# insert column detection here, replace that slice
            # pull out just dates
            ls_dates = [i[1] for i in lake[4]]
            # objectify landsat dates
            #objectified_ls_dates = objectify_dates(ls_dates, 1)
            objectified_ls_dates = objectify_dates(ls_dates, 1)
            ls_scene_list = []
            # sticks scene names and date objects together
            #print 'len check scenes/dates {} obj dates {}'.format(len(lake[3]), len(objectified_ls_dates))
            for (i, x) in izip(lake[4], objectified_ls_dates):
                ls_scene_list.append([i[0], i[1], x])

# or zip what fits the criteria
            acceptable_bump_dates = map(lambda x: (x- settings['date_allowance'], x, \
                x + settings['date_allowance']), lake[3])
            # flatten list
            #acceptable_bump_dates_list = flatten(acceptable_bump_dates)
            acceptable_bump_dates_list = flatten.main(acceptable_bump_dates)
            log.debug('acceptable_bump_dates_list: {}'.format(acceptable_bump_dates_list))

            acceptable_scenes = [i for i in ls_scene_list if i[2] in acceptable_bump_dates_list]
# !!! it's an iterchain, can only be used once!

            log.debug('acceptable scenes len {}, {}'.format(len(acceptable_scenes), acceptable_scenes))
            scene_count += len(acceptable_scenes)

            l_result.append(acceptable_scenes)
        # no data
        else:
            l_result.append('')
            log.debug('no acceptable_bump_dates found')
        result.append(l_result)

    log.debug('total images selected: {}'.format(scene_count))
    return result

def code_images(args):
    """
    Input is a complex list. Output is a sorted, simple, coded list for output as CSV.

    """
    log = module_logger.getChild(__name__)
    #log = logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    result = []
    scene_selection = args

    # code is 2 factor:
    # lake id-
    # scenes are double digit

    # initialize alpha codes
    #lake_code = string.ascii_uppercase[0] *2
    #log.debug('initialized lake code: {}'.format(lake_code))
    #input format: 'lake name, lake files, dates object list,
    #               [scenes, scene_dates. scene_date_objects, * that are within date_allowance]', *]
    result.append(settings['Coded_Headers'])

    for lake in scene_selection:
        lake_id = lake[0]
        lake_name = lake[1]

        # check for selection before coding
        if lake[4]:
            for x, i in enumerate(lake[4], start=1):
                log.debug('{}: iterating {}'.format(x, i))
                scene_name = i[0]
                scene_date = i[1]
                l_result = [lake_id + '-' + str(x).zfill(2), lake_id, lake_name, scene_name, scene_date]
                result.append(l_result)
                log.debug('line result: {}'.format(l_result))

##            # increment the lake code
##            increment = chr(ord(lake_code[-1]) +1)
##            lake_code = lake_code[0] + increment
##            # if you go past 'Z', reset to 'A', and increment other one
##            if ord(increment) > 90:
##                increment = 'A'
##                other_increment = chr(ord(lake_code[0]) +1)
##                lake_code = other_increment + increment
##            log.debug('incremented lake code: {}'.format(lake_code))
        else:
            log.debug('lake {} had no selected scenes, skipping'.format(lake_name))
    log.debug('result: {}'.format(result))
    return result

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
#from variables import *
import csv

def write_csv_output(input, path, output_name):
    """
    Writes the output to a csv file.
        Args:
            args (list): list of lists to write
            path: file path to write to
            output_name: name of csv output

    """
    # write output into output folder
    log = module_logger.getChild(__name__)
    #log.debug('with: {}'.format(args))
    log.debug('with: {} {} {}'.format(input, path, output_name))
    #output_path = working_dir, settings['Processed Dir'], settings['Final Output']
    output_file = os.path.join(path, output_name)
    log.debug('output saving as: {}'.format(output_file))
    log.debug('output_file type: {}'.format(type(output_file)))
    with open(os.path.join(output_file), 'wb') as file:
    #with open(os.path.join(working_dir, settings['Output Dir'], settings['Final Output']), 'wb') as file:
        writer = csv.writer(file) #quotechar='"', quoting=csv.QUOTE_ALL, dialect='excel')
        writer.writerows(input)
        log.info(u'result saved as {}'.format(output_file))



def main():
    pass

if __name__ == '__main__':
    main()
