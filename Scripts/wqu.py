#-------------------------------------------------------------------------------
# Name:        wq script utility functions
# Purpose:
#
# Author:      clay barrett
#
# Created:     14/05/2016
# Copyright:   (c) clay barrett 2016
# Licence:     see License.txt
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('for {} in {}'.format(search_term, list))
    result = []

    # see if term is in the list
    term_row = [idx for idx in list if search_term in idx]
    log.debug('term row: {}'.format(term_row))
    # possible outcomes are 0 = not found, 1 = expected to find one time, >1 = problem...
    # this is a list comp, so result is a list. check length!
    if len(term_row) == 1:
        # get the index of the row term was found in
        term_row_i = [i for i, x in enumerate(list) if search_term in x][0]
        log.debug('term row_i: {}'.format(term_row_i))
        # get the column index it was found in
        term_col_i = [x for x, i in enumerate(term_row[0]) if i == search_term][0]  # returns i as list, so [0]
        log.debug('term col_i: {}'.format(term_col_i))
        # with start row and col found, find last col and return a selection
        # only get from header row +1 to last row
        # can deal with an extra, blank line result on return
        selection = [x[term_col_i] for idx, x in enumerate(list) if idx in range(term_row_i +1, len(list))] # but this leaves blank on the end of names
        log.debug('list_find selection dump: {}'.format(selection)) # should gaurd against blanks above header row

        # stick header on if desired
        if header:
            selection.insert(search_term, 0)
            [[term_row_i, term_col_i], selection]
        else:
            result = [[term_row_i, term_col_i], selection]
    else:
        log.warning(u'Failed to find {} exactly once in list'.format(search_term))
        result = [0, 0]
    log.debug('list_find result: {}'.format(result))
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
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

    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
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
#import flatten
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
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
            acceptable_bump_dates_list = flatten(acceptable_bump_dates)
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
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
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with: {} {} {}'.format(input, path, output_name))

    output_file = os.path.join(path, output_name)
    log.debug('output saving as: {}'.format(output_file))
    log.debug('output_file type: {}'.format(type(output_file)))
    with open(os.path.join(output_file), 'wb') as file:
    #with open(os.path.join(working_dir, settings['Output Dir'], settings['Final Output']), 'wb') as file:
        writer = csv.writer(file) #quotechar='"', quoting=csv.QUOTE_ALL, dialect='excel')
        writer.writerows(input)
        log.info(u'result saved as {}'.format(output_file))
    # ends no return

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
import csv

def read_csv(args):
    """Reads csv files.
        Args:
            args (file path): csv file to open and read
        Returns:
            result (list): bulk input from csv

    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug(u'with {}'.format(args))
    result = []

    with open(args, 'rb') as csvfile:
        log.debug(u'Reading {} as csvfile.'.format(args))
        reader = csv.reader(csvfile)
        for row in reader:
            result.append(row)
            #log.debug(u'Collecting {} to result.'.format(row))

    return result

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

# code adapted from J Gomez-Dans <j.gomez-dans@ucl.ac.uk>
def get_metadata_name(fname):
    """ ! Not valid after 2016 changes!
    This function takes `fname`, a filename (opionally with a path), and
    and works out the associated metadata file
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))

    original_fname = os.path.basename ( fname )
    metadata_fname = original_fname.split("_")[0] + "_MTL.txt"
    metadata_fname = os.path.join ( os.path.dirname ( fname ), metadata_fname )
    log.debug('{} -> {}'.format(fname, metadata_fname))

    return metadata_fname

import os
import logging
import inspect
from variables import *
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

# code adapted from J Gomez-Dans <j.gomez-dans@ucl.ac.uk>
def process_metadata(fname, scene):
    """A function to extract the relelvant metadata from the
    USGS control file. Returns dicionaries with LMAX, LMIN,
    QCAL_LMIN and QCAL_LMAX for each of the bands of interest."""
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    #log.debug('with {}'.format(fname))

    #fp = open( fname, 'r') # Open metadata file
    # band vars
    lmax = {} # Dicts to store constants
    lmin = {}
    qc_lmax = {}
    qc_lmin = {}
    gain = {}
    bias = {}
    p_list = ['lmax', 'lmin', 'qc_lmax', 'qc_lmin', 'gain', 'bias']
    parameters = [lmax, lmin, qc_lmax, qc_lmin, gain, bias]

    # scene vars
    sun_elevation = 0
    julian_date = 0
    sat_id = ''
# should these be none?

    # lets just go ahead and assemble these as a dict for return
    result = {}

#get it written with names here, and we're good till DNmin
    #result[scene] = bias, gain, lmax, lmin, qc_lmax, qc_lmin

# what if instead, these strings are in a dict
# then iterate through them, using a list comp to pull just one set
# then select by 'Pick Bands' to add dict vals
    with open(fname, 'r') as fp: # Open metadata file
        for line in fp: #
          log.debug('reading line: {}'.format(line))
          # Check for LMAX and LMIN strings
          # Note that parse logic is identical to the first case
          # This version of the code works, but is rather inelegant!

##          s = line.split("=") # Split by equal sign
##          # test this one
##          the_band = s[0].strip()[-1] # Band number as string
##          # use this as dict name
##          band_name = 'B' + s[0].split("_")[3].strip()

          if ( line.find ("RADIANCE_MULT_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #logger.debug('split line: {}, band check: {}'.format(s, int(s[0].split("_")[3])))
##              #integer necessary? the_band = int(s[0].split("_")[3]) # Band number as integer
##              ##print '.' + s[0].split("_")[3] + '.'
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[3].strip() + '.TIF'
##              # leave stuff after band name to make dict calls easy
##              band_name = 'B' + s[0].split("_")[3].strip()
##              print s[0]
##              print s[0].split("_")[3]
##              print s[0].split("_")[3].strip()

##              ##the_band = s.partition('=')[0].strip()[-1]
##              ##print type(the_band)
##              ##print type(settings['Pick Bands'][0])
              log.debug('test {} in {}: {}'.format(the_band, settings['Pick Bands'], the_band in settings['Pick Bands']))
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  gain[band_name] = float ( s[-1] ) # Get constant as float
                  log.debug('gain[{}]: {}'.format(band_name, gain[band_name]))
          elif ( line.find ("RADIANCE_ADD_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #the_band = int(s[0].split("_")[3]) # Band number as integer
##              the_band = s[0].strip()[-1] # Band number as string
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[3].strip()
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  bias[band_name] = float ( s[-1] ) # Get constant as float
          elif ( line.find ("QUANTIZE_CAL_MAX_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #the_band = int(s[0].split("_")[4]) # Band number as integer
##              the_band = s[0].strip()[-1] # Band number as string
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[4].strip()
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  qc_lmax[band_name] = float ( s[-1] ) # Get constant as float
          elif ( line.find ("QUANTIZE_CAL_MIN_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #the_band = int(s[0].split("_")[4]) # Band number as integer
##              the_band = s[0].strip()[-1] # Band number as string
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[4].strip()
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  qc_lmin[band_name] = float ( s[-1] ) # Get constant as float
          elif ( line.find ("RADIANCE_MAXIMUM_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #the_band = int(s[0].split("_")[3]) # Band number as integer
##              the_band = s[0].strip()[-1] # Band number as string
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[3].strip()
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  lmax[band_name] = float ( s[-1] ) # Get constant as float
          elif ( line.find ("RADIANCE_MINIMUM_BAND") >= 0 ):
              s = line.split("=") # Split by equal sign
##              #the_band = int(s[0].split("_")[3]) # Band number as integer
##              the_band = s[0].strip()[-1] # Band number as string
              # test this one
              the_band = s[0].rpartition('_')[2].strip() # Band number as string
              # use this as dict name
              band_name = 'B' + s[0].split("_")[3].strip()
              if the_band in settings['Pick Bands']: # Is this one of the bands we want?
                  lmin[band_name] = float ( s[-1] ) # Get constant as float
          elif ( line.find('SUN_ELEVATION') >= 0):
              s = line.split('=')
              sun_elevation = s[1].strip()
              log.debug('sun_elevation: {}'.format(sun_elevation))

    # other is scene wide metadata
    sat_id = scene[2]
    julian_date = scene[13:16]
    log.debug('sat id: {}, julian_date: {}'.format(sat_id, julian_date))
    other = [sat_id, julian_date, sun_elevation]

    # make named dict for result
    for (x, i) in zip(p_list, parameters):
    	result[x] = i
    log.debug('result: {}'.format(result))
    #return ( bias, gain, lmax, lmin, qc_lmax, qc_lmin )
    #result
    return [result, other]

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
from arcpy.sa import *
from variables import *

# code adapted from Steve Kochaver kochaver.python@gmail.com
def calc_radiance (LMAX, LMIN, QCALMAX, QCALMIN, QCAL, outfolder):
#def calc_radiance (args):
    """
    Calculate the TOA radiance from metadata on each band.
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))

    #args = (LMAX, LMIN, QCALMAX, QCALMIN, QCAL, outfolder)
    log.debug('with {}/{} {}/{} {}/{}'.format(LMAX, LMIN, QCALMAX, QCALMIN, QCAL, outfolder))
    LMAX = float(LMAX)
    LMIN = float(LMIN)
    QCALMAX = float(QCALMAX)
    QCALMIN = float(QCALMIN)
    offset = (LMAX - LMIN)/(QCALMAX-QCALMIN)
    input_ras = Raster(QCAL)

    outname = os.path.join(outfolder, (settings['Rad Prefix'] + QCAL.split('\\')[-1]))
    log.debug('output name: {}'.format(outname))

    out_raster = (offset * (input_ras - QCALMIN)) + LMIN
    log.debug('saving output as: {}'.format(outname))
    out_raster.save(outname)

    return outname

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

# code adapted from Steve Kochaver kochaver.python@gmail.com
def get_ESUN(bandNum, SIType):
    """
    Gets parameters based on band and Landsat model
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))

    SIType = SIType
    ESUN = {}
#SENSOR_ID = "OLI_TIRS"
#LS 8 from http://semiautomaticclassificationmanual.readthedocs.org/en/latest/Landsat_conversion.html
#ESUN=(pi*d^2)*RADIANCE_MAXIMUM/REFLECTANCE_MAXIMUM
#where RADIANCE_MAXIMUM and REFLECTANCE_MAXIMUM are provided by image metadata.

#or http://landsat.usgs.gov/ESUN.php says
# ESUN values are not required for reflectance conversion.
    #from NASA's Landsat7 User Handbook Table 11.3 http://landsathandbook.gsfc.nasa.gov/pdfs/Landsat7_Handbook.pdf
    #ETM+ Solar Spectral Irradiances(generated using the Thuillier solar spectrum)
    #if SIType == 'ETM+ Thuillier':
    if SIType == '7':
        ESUN = {'B1':1997,'B2':1812,'B3':1533,'B4':1039,'B5':230.8,'B7':84.90,'B8':1362}

##    #from NASA's Landsat7 User Handbook Table 11.3 http://landsathandbook.gsfc.nasa.gov/data_prod/prog_sect11_3.html
##    #ETM+ Solar Spectral Irradiances (generated using the combined Chance-Kurucz Solar Spectrum within MODTRAN 5)
##    if SIType == 'ETM+ ChKur':
##        ESUN = {'b1':1970,'b2':1842,'b3':1547,'b4':1044,'b5':225.7,'b7':82.06,'b8':1369}
##
##    #from NASA's Landsat7 User Handbook Table 9.1 http://landsathandbook.gsfc.nasa.gov/pdfs/Landsat7_Handbook.pdf
##    #from the LPS ACCA algorith to correct for cloud cover
##    if SIType == 'LPS ACAA Algorithm':
##        ESUN = {'b1':1969,'b2':1840,'b3':1551,'b4':1044,'b5':225.7,'b7':82.06,'b8':1368}

    #from Revised Landsat-5 TM Radiometric Calibration Procedures and Postcalibration
    #Dynamic Ranges Gyanesh Chander and Brian Markham. Nov 2003. Table II. http://landsathandbook.gsfc.nasa.gov/pdfs/L5TMLUTIEEE2003.pdf
    #Landsat 4 ChKur
    #if SIType == 'Landsat 5 ChKur':
    if SIType == '5':
        ESUN = {'B1':1957,'B2':1825,'B3':1557,'B4':1033,'B5':214.9,'B7':80.72}

    #from Revised Landsat-5 TM Radiometric Calibration Procedures and Postcalibration
    #Dynamic Ranges Gyanesh Chander and Brian Markham. Nov 2003. Table II. http://landsathandbook.gsfc.nasa.gov/pdfs/L5TMLUTIEEE2003.pdf
    #Landsat 4 ChKur
    if SIType == 'Landsat 4 ChKur':
        ESUN = {'b1':1957,'b2':1826,'b3':1554,'b4':1036,'b5':215,'b7':80.67}

    bandNum = str(bandNum)

    return ESUN[bandNum]

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
from arcpy.sa import *
from variables import *
import math

def calc_reflectance(solarDist, ESUN, solarElevation, radianceRaster, Lhaze, outfolder):
    """
    COSTDOS correction in the Lhaze parameter. Toggle Global var Do_Costdos to use 0.0.
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with: {} {} {} {} {} {}'.format(solarDist, ESUN, solarElevation, radianceRaster, Lhaze, outfolder))

    outname = os.path.join(outfolder, (settings['Corr Prefix'] + radianceRaster.split('\\')[-1]))
    #Value for solar zenith is 90 degrees minus solar elevation (angle from horizon to the center of the sun)
    #http://landsathandbook.gsfc.nasa.gov/data_properties/prog_sect6_3.html
    solarZenith = (90.0 - float(solarElevation)) * (math.pi / 180)    #Converted from degrees to radians
    ##solarZenith = math.pow(((90.0 - float(solarElevation))* (math.pi / 180)), 2)
    solarDist = float(solarDist)
    ESUN = float(ESUN)
    radiance = Raster(radianceRaster)

    ##outraster = (math.pi * radiance * math.pow(solarDist, 2)) / (ESUN * math.cos(solarZenith)) * scaleFactor
    outraster = (math.pi * (radiance - Lhaze) * math.pow(solarDist, 2)) / (ESUN * math.cos(solarZenith))
    outraster.save(outname)
    log.debug('output saved: {}'.format(outname))

    return outname

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
import arcpy
from arcpy.sa import *
from variables import *

def b6_to_temp(sat_id, radiance_ras):
    """
Appears to return outname... no matter what
    Converts B6 as radiance to temperature in Kelvins
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with {}'.format(radiance_ras))
    result = []

    name = radiance_ras.rsplit('\\', 1)
    desc=arcpy.Describe(radiance_ras)
    log.debug('no data = {}/{}'.format(desc.noDataValue, type(desc.noDataValue)))

    input_ras = Raster(radiance_ras, noDataValue=desc.noDataValue)
    outname = os.path.join(name[0], (settings['Temp Prefix'] + name[1]))
    log.debug('converted B6 output name: {}'.format(outname))
    #from http://landsathandbook.gsfc.nasa.gov/data_prod/prog_sect11_3.html 11.5
    calibration_constants = {
    '7': [666.09, 1282.71],
    '5': [607.76, 1260.56]
    }
    # LS8 https://landsat.usgs.gov/documents/Landsat8DataUsersHandbook.pdf 5.3: read from MD


# can be done as array, but have to handle spatial data seperatly
##    input_arr = arcpy.RasterToNumPyArray(radiance_ras, nodata_to_value=np.NaN)
##    print input_arr
##    d1 = (calibration_constants[sat_id][0] / input_arr) +1
##    print d1
##    n = calibration_constants[sat_id][1] / np.log(d1)
##    print n
# from http://landsathandbook.gsfc.nasa.gov/data_prod/prog_sect11_3.html
#T = (k2)/(ln(k1/L +1))
#Where:
#T	=   Effective at-satellite temperature in Kelvin
#K2	=   Calibration constant 2 from Table 11.5
#K1	=   Calibration constant 1 from Table 11.5
#L	=   Spectral radiance in watts/(meter squared * ster * ????m)
   # print 'types', type(calibration_constants[sat_id][0]), type(input_ras), type(1)
    d1 = (calibration_constants[sat_id][0] / input_ras) +1
##    print calibration_constants[sat_id][0], calibration_constants[sat_id][1], type(calibration_constants[sat_id][0])
##    d1 = calibration_constants[sat_id][0] * (1/ radiance_ras)
    out_raster = calibration_constants[sat_id][1] / Ln(d1)
        ##(math.log((calibration_constants[sat_id][0] / input_ras) +1))
        ##(math.log(float(calibration_constants[sat_id][0] / input_ras +1))) #(math.log(d1) +1)
    log.debug('saving output as: {}'.format(outname))
    out_raster.save(outname)

    return outname

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
import arcpy
from arcpy.sa import *
from variables import *

def ndwi_mask(folder_path, B2, B5):
    """
    Accepts the path and two required bands as inputs (2 & 5)
    Creates ndwi_Imagename.TIF in same dir
    Returns status code
    Water = 1 (True), otherwise false (0)
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('in {} with {} and {}'.format(folder_path, B2, B5))
    result = 0
    output_name = os.path.join(folder_path, settings['NDWI Prefix'] + B2.rsplit('_', 1)[0] + '.TIF')

    Green = os.path.join(folder_path, B2)
    NIR = os.path.join(folder_path, B5)

    #Create Numerator and Denominator rasters as variables and NDVI output (note that arcpy.sa.Float returns a floating point raster)
    numerator = arcpy.sa.Float(Raster(Green) - Raster(NIR))
    denominator = arcpy.sa.Float(Raster(Green) + Raster(NIR))
    log.debug("Dividing NDWI Rasters")
    NDWI_eq = arcpy.sa.Divide(numerator, denominator)
    # save intermidiate before reclass
    NDWI_eq.save(os.path.join(folder_path, 'raw_ndwi.TIF'))

    # Reclassify to get a mask
### should probably define this parameter in DICT
    result = Reclassify(NDWI_eq, "Value", RemapRange([[-2, 0, 0], [0.01, 2, 1]]))

    #Saving output to result output you specified above
    try:
        result.save(output_name)
        log.debug("NDWI Successful: {}".format(output_name))
        result = output_name
    except:
        log.debug("NDWI Unsuccessful: {}".format(result))
        result = 0

    return result

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

#import arcpy

def get_raster_count(args):
    """
    Subfunction to deal with ensuring there's a RAT and then getting the cell
    count out of the table using a cursor.
    Accepts raster.
    Returns integer value of total cell count or -1.
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    #log.debug('with {}'.format(args))
    result = -1

    x = args

    # see if it's all null before the bother
    null_result = arcpy.GetRasterProperties_management(x, 'ALLNODATA')
    # make result useful
    null = bool(int(null_result.getOutput(0)))
    if null:
        result = 0
    else:
    # bother
        if x.hasRAT:
            log.debug('{} has RAT'.format(x))
        else:
            #logger.debug('{} has no RAT'.format(x))
            try:
                arcpy.BuildRasterAttributeTable_management(x, "OVERWRITE")
            except:
                log.debug('{} failed to build RAT'.format(x))

        # now RAT is built
        total_count = 0
        with arcpy.da.SearchCursor(os.path.join(x.path, x.name), ["VALUE", "COUNT"]) as rows:
            for row in rows:
                print row
                # nodata isn't in the RAT, so no need to filter it out
                # but we do skip the 0's
                if row[0] > 0:
                    total_count += row[1]
        result = float(total_count)

    log.debug('{} result = {}'.format(x.name, result))
    return result


import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
import arcpy
from arcpy.sa import *
#import get_raster_count
from variables import *

def not_cloud_mask(*args):
    """
    0 = cloud, 1 = not cloud, 2 = ambig
    Return path of mask?
    """
# needs cleaned up, possibly could be simplified with iterators
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with {}'.format(args))
    result = -1

    folder_path = args[0]
    images = [os.path.join(folder_path, i) for i in args[1]]
    #print images
    B2 = Raster(images[0])
    B3 = Raster(images[1])
    B4 = Raster(images[2])
    B5 = Raster(images[3])
    B6 = Raster(images[4])

    # save final mask in folder_path dir
    # all the other stuff can be in the cloud_mask dir
    working_path = os.path.join(folder_path, settings['Cloud Dir'])
    # make sure the output dir exists, if not, make
    out = working_path
    if not os.path.exists(out):
        os.makedirs(out)

    #total_count = 0
    nodata_value = arcpy.Describe(B2).noDataValue
    output_name = os.path.join(folder_path, 'not_cloud.TIF')
    agg_name = os.path.join(working_path, 'agg_mask.TIF')
    name_0 = os.path.join(working_path, 'cloud.TIF')
    name_1 = os.path.join(working_path, 'not_cloud.TIF')
    name_2 = os.path.join(working_path, 'ambig.TIF')

    sql_exp = """{} > {}""".format(arcpy.AddFieldDelimiters(B2, 'VALUE'), nodata_value)
    log.debug('test sql statement: {}'.format(sql_exp))
    # test_raster just to get in count of data pixels
    test_raster = Test(B2, sql_exp)
    test_raster.save(os.path.join(working_path, 'test.TIF'))
    total_count = get_raster_count(test_raster)
    test_raster = Con(test_raster > 0, 0)

    # count of raster cells by type
    snow_count = 0
    hot_count = 0
    cold_count = 0
    desert_count = 0

    # filter 1: B3 < .08 = 1/not cloud
#Con (in_conditional_raster, in_true_raster_or_constant, {in_false_raster_or_constant}, {where_clause})
    #f1 = Con(B3 < .08, 1, -1)
    f1 = Con(B3 < .08, 1, 0)
    f1.save(os.path.join(working_path, 'f1.TIF'))
    # add f1 to an aggregate mask
    ## also to the 1 mask
    # add to aggregate (binary mask, assigned a category or not)
    agg = BooleanOr(f1, test_raster)
    agg.save(os.path.join(working_path, 'agg1.TIF'))
    ## add to cloud name_1
    ##mask_1 = BooleanOr(f1, test_raster)
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(agg)
    sum_2 = 0
    log.debug('f1- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 2: NSDI (B2 - B5)/(B2 + B5). >.7 = 1/not cloud (get count of snow?)
    NSDI = ((B2 - B5)/(B2 + B5))
    #NSDI.save(os.path.join(folder_path, 'NSDI.TIF'))
    sf2 = Con(NSDI > 0.7, 1, 0)
    sf2.save(os.path.join(working_path, 'sf2.TIF'))

    # just f2 additions
    f2 = Diff(sf2, agg)
    f2.save(os.path.join(working_path, 'f2.TIF'))

    # get snow count
    get_raster_count(f2)

    # use aggregate + selection
    agg = BooleanOr(agg, sf2)
    #agg2 = (f1 + f2)
    #agg2.save(os.path.join(folder_path, 'agg2.TIF'))
    agg.save(os.path.join(working_path, 'agg2.TIF'))
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(agg)
    sum_2 = 0
    log.debug('f2- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

# you sould consider that you may have failed to convert to Kelvins properly
    # filter 3: B6 > 300K = 1
    #f3 = B6 > 300, 1, 0
    #f3.save(os.path.join(folder_path, 'rf3.TIF'))
    sf3 = Con(B6 > 300, 1, 0)
    sf3.save(os.path.join(working_path, 'sf3.TIF'))
    f3 = Diff(sf3, agg)
    f3.save(os.path.join(working_path, 'f3.TIF'))
    # add to aggregate (binary mask, assigned a category or not)
    #agg = BooleanOr(f1, agg_name)
    agg = BooleanOr(agg, sf3)
    agg.save(os.path.join(working_path, 'agg3.TIF'))
##    agg2 = (f1 + f2 + f3)
##    agg2.save(os.path.join(folder_path, 'agg32.TIF'))
    # copy not-cloud aggregate to (name_1) since switching to detect Ambig
    mask_1 = agg
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(mask_1)
    sum_2 = 0
    log.debug('f3- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 4: B5/6 Composite (1 - B5) * B6 > 225 = 2
    f4 = ((1 - B5) * B6)
    f4.save(os.path.join(working_path, 'rf4.TIF'))
    sf4 = Con(((1 - B5) * B6) > 225, 1, 0)
    sf4.save(os.path.join(working_path, 'sf4.TIF'))

    f4 = Diff(sf4, agg)
    f4.save(os.path.join(working_path, 'f4.TIF'))

    # add to aggregate
    agg = BooleanOr(agg, sf4)
    agg.save(os.path.join(working_path, 'agg4.TIF'))
    # start to cloud name_1 (ambig)
    mask_2 = f4
    mask_2.save(os.path.join(working_path, 'f4_a.TIF'))
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(mask_1)
    sum_2 = get_raster_count(mask_2)
    print sum_1, sum_2, total_count, sum_2/total_count
    log.debug('f4- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 5: B3/4 Ratio (B4/B3) > 2.0 = 2
    sf5 = Con((B4/B3) > 2.0, 1, 0)
    sf5.save(os.path.join(working_path, 'sf5.TIF'))
    f5 = Diff(sf5, agg)
    f5.save(os.path.join(working_path, 'f5.TIF'))
    # add to aggregate (binary mask, assigned a category or not)
    agg = BooleanOr(agg, f5)
    agg.save(os.path.join(working_path, 'agg5.TIF'))
    # add to cloud name_1 (ambig)
    mask_2 = BooleanOr(mask_2, f5)
    mask_2.save(os.path.join(working_path, 'f5_a.TIF'))
    # running sums
    print 'mask checks:', type(mask_1), mask_1.path, mask_1.name, type(mask_2), mask_2.path, mask_2.name
    sum_0 = 0
    sum_1 = get_raster_count(mask_1)
    sum_2 = get_raster_count(mask_2)
    log.debug('f5- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 6: B4/2 Ratio (B4/B2) > 2.0 = 2
    sf6 = Con((B4/B2) > 2.0, 1, 0)
    sf6.save(os.path.join(working_path, 'sf6.TIF'))
    f6 = Diff(sf6, agg)
    f6.save(os.path.join(working_path, 'f6.TIF'))

    # add to aggregate
    agg = BooleanOr(agg, f6)
    # add to cloud name_1
    mask_2 = BooleanOr(f6, mask_2)
    mask_2.save(os.path.join(working_path, 'f6_a.TIF'))
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(mask_1)
    sum_2 = get_raster_count(mask_2)
    log.debug('f6- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 7: B4/5 Ratio (B4/B5) > 1.0 = 2 (get count of desert pixels)
    sf7 = Con((B4/B5) > 1.0, 1, 0)
    sf7.save(os.path.join(working_path, 'sf7.TIF'))
    f7 = Diff(sf6, agg)
    f7.save(os.path.join(working_path, 'f7.TIF'))
    desert_count = get_raster_count(f7)
    log.debug('desert_count: {}'.format(desert_count))

    # add to aggregate (binary mask, assigned a category or not)
    agg = BooleanOr(f7, agg)
    # add to cloud name_1
    mask_2 = BooleanOr(f7, mask_2)
    mask_2.save(os.path.join(working_path, 'f7_a.TIF'))
    # running sums
    sum_0 = 0
    sum_1 = get_raster_count(mask_1)
    sum_2 = get_raster_count(mask_2)
    log.debug('f7- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    # filter 8: Everything remaining = 0 (hot and cold clouds)
    sf8 = BooleanNot(agg)
    sf8.save(os.path.join(working_path, 'sf8.TIF'))
    f8 = Diff(sf8, agg)
    f8.save(os.path.join(working_path, 'f8.TIF'))

    mask_0 = sf8
    # running sums
    sum_0 = get_raster_count(mask_0)
    sum_1 = get_raster_count(mask_1)
    sum_2 = get_raster_count(mask_2)
    log.debug('f8- {:.3f} cloud. {:.3f} not cloud. {:.3f} ambig.'.format(sum_0 / total_count,  \
        sum_1 / total_count, sum_2 / total_count))

    #finalize masks
    log.debug('saving masks')
    agg.save(agg_name)
    ## didn't add anything, so just take f8
    mask_0.save(name_0)
    mask_1.save(name_1)
    mask_2.save(name_2)

    # pass 2: only fires if there's clouds and all the 3 checks fail
    # if mask_1 is all 0: cloudfree, else try checks
    c_mask_0 = SetNull(mask_0, 1, "VALUE = 0")
    null_result = arcpy.GetRasterProperties_management(mask_0, 'ALLNODATA')
    null = bool(int(null_result.getOutput(0)))
    log.debug('{} all no data check: {}'.format(f8.name, null))
    if null:
    # no clouds = no pass 2
        log.debug('no clouds, no pass 2')
        result = 1
    else:
        check = 0   # if check == 3, do pass 2

        # if desert_index > 0.5: increment check
        if desert_count == 0:
            desert_index = 0
        else:
            #print desert_count, type(desert_count), total_count, type(total_count)
            desert_index = desert_count / total_count
        if desert_count > 0.5:
            check += 1
            log.debug('di did trip'.format(desert_index))
        else:
            log.debug('di did not trip'.format(desert_index))

##        if desert_count:
##            #print desert_count, type(desert_count), total_count, type(total_count)
##            desert_index = desert_count / total_count
##            if desert_count > 0.5:
##                check += 1
##                log.debug('di did trip'.format(desert_index))
##            else:
##                log.debug('di did not trip'.format(desert_index))
##        else:
##            log.debug('des coutn fail: {}'.format(desert_count))
# index fails if count == 0. so just set it to 0
        # if cold clouds are < .4%, skip pass 2
        if cold_count == 0:
            cold_index = 0
        else:
            # use B6 since it's the truest count of final image/filter
            cold_index = cold_count / get_raster_count(B6)
        if cold_index  < .004:
            log.debug('ci count did not trip'.format(cold_index))
        else:
            check += 1
            log.debug('ci did trip'.format(cold_index))

##        # if cold clouds are < .4%, skip pass 2
##        if cold_count:
##            # use B6 since it's the truest count of final image/filter
##            cold_index = cold_count / get_raster_count(B6)
##            if cold_index  < .004:
##                log.debug('ci count did not trip'.format(cold_index))
##            else:
##                check += 1
##                log.debug('ci did trip'.format(cold_index))
##        else:
##            log.debug('could count fail: {}'.format(cold_count))
        # if mean temp of cold class < 295K
        #cold_mean = GetRasterProperties_management (in_raster, {property_type}, {band_index}) / cold_count
        cold_mean_result = arcpy.GetRasterProperties_management(ExtractByMask(B6, f8), 'MEAN')
    # maybe extract B6 using Mask_0, and then get average from this.
        cold_mean = float(cold_mean_result.getOutput(0))
    # but just to test, really have to decide which.
        # NO cold_mean = float(cold_mean_result.getOutput(0))/ cold_count
        if cold_mean < 295:
            check += 1
            log.debug('cm count did trip'.format(cold_mean))
        else:
            log.debug('cm did not trip'.format(cold_mean))

        if check == 3:
            log.debug('need pass 2')
            # setup some vars and things

            # split cloud into warm and cold for p2
            # B5/6 Composite (1 - B5) * B6 > 210 = warm, rest are cold
            warm_cloud = Con((f8 * ((1 - B5) * B6)) > 210, 1, 0)
            warm_cloud.save(os.path.join(working_path, 'warm_cloud.TIF'))
            warm_count = get_raster_count(hot_cloud)
            cold_cloud = Con((f8 * ((1 - B5) * B6)) <= 210, 1, 0)
            cold_cloud.save(os.path.join(working_path, 'cold_cloud.TIF'))
            cold_count = get_raster_count(cold_cloud)

            #selection of which cloud groups to process
            if snow_count:
                snow_pct = snow_count / total_count
                log.debug('snow_pct: {}'.format(snow_pct))
                # just assign the selection to a var
                if snow_pct < .01:
                    # snow-free, use hot_cloud and cold_cloud AKA mask_0
                    log.debug('snow free, use mask_0')
                    test_clouds = mask_0
                else:
                    # use cold cloud only only
                    log.debug('snow found, use cold and move warm to ambig')
                    test_clouds = cold_cloud
                    # reassign warm_cloud as ambig/mask_2
                    mask_2 = BooleanOr(mask_2, warm_cloud)
                    mask_2.save(os.path.join(working_path, 'ambigP2.TIF'))
            else:
                log.warning('no snow_count: {}'.format(snow_count))

            # with selection, run through threshold evaluation
            #arcpy.CalculateStatistics_management(test_clouds)
            raster_stats = CalculateStatistics_management(test_clouds, "MINIMUM")
            log.debug('cloud min', raster_stats.getOutput(0))

            test_arr = np.array(test_clouds)
            min = np.amax(test_arr)
            max = np.amin(test_arr)
            std_dev = np.std(test_arr)
            mean = np.mean(test_arr)
            n = 0
            for i in test_arr:
                n += (i - mean)^3
            skew = n / std_dev
            skew = sp.stats.skew(test_arr)
            #slope1 = x - mean
            #slope =
            log.debug('min {}. max {}. mean {}. std dev {}. skew {}'.format( \
                min, max, std_dev, skew))

            #skew = .3       # -1 to 1
            max_threshold = (max - min) * .9875 + min
            high_threshold = (max - min) * .975 + min
            low_threshold = (max - min) * .825+ min

            if skew > 0:
                # no adjustment to thresholds
                log.debug('no shift')
            else:
                # adjust thresholds up
                shift_factor = skew * std_dev
                log.debug('shift factor: {}'.format(shift_factor))
                low_threshold = low_threshold * shift_factor
                high_threshold = high_threshold * shift_factor

                # back high down if too high
                ##pct9875 =
                if high_threshold > max_threshold:
                    high_threshold = max_threshold
                    log.debug('high_threshold reassigned: {}'.format(high_threshold))
                else:
                    log.debug('high_threshold passed: {}'.format(high_threshold))

            # Evaluate "termal effects"
            g1 = Con(test_clouds < high_threshold, 1, 0)
            g1 = Con(test_clouds < low_threshold, 0, g1)
            g2 = Con(g1 < low_threshold, 1, 0)

            # compute stats for upper
            ##g1_arr = np.array(g1)
            g1_count = get_raster_count(g1)
            g12 = np.count_nonzero(~np.isnan(np.array(g1)))
            log.debug('count test: {} vs {}'.format(g1_count, g12))
            g1_pct = g1_count / total_count
            g1_mean = np.mean(g1)

            if g1_pct > .40 or g1_mean > 295:
                # then g1 are classified non-cloud
                mask_1 = BooleanOr(mask_1, g1)
                log.debug('rejecting g1')
                # continue to evaluate g2
                # compute stats for lower
                ##g2_arr = np.array(g2)
                g2_count = get_raster_count(g2)
                g2_pct = g2_count / total_count
                g2_mean = np.mean(g2)

                if g2_pct > .40 or g2_mean > 295:
                    # then all ambig are scrapped
                    mask_1 = BooleanOr(mask_1, mask_2)
                    log.debug('rejecting all ambig')
                else:
                    # accept the lower group into cloud
                    mask_0 = BooleanOr(mask_0, g2)
                    log.debug('uniting the lower with cloud')
            else:
                # unite all 3 cloud classes
                log.debug('uniting the 3 cloud classes')
                mask_0 = BooleanOr(mask_0, mask_2)

            # CHEQUES
            mask_0.save(os.path.join(working_path, 'cloudP2.TIF'))
        else:
            log.debug('at least one check was breeched, no Pass 2.')
            mask_0 = BooleanOr(test_raster, B6)
            #mask_0.save(os.path.join(folder_path, 'ambigP2b.TIF'))
        # save final masks
        mask_0.save(os.path.join(working_path, 'cloudP2.TIF'))
        mask_1.save(os.path.join(working_path, 'not_cloudP2.TIF'))
        #mask_2.save(name_2)
        result = 2
    # and then cloud holes filled in?
        mask_1.save(output_name)
    log.debug('not cloud mask named {}'.format(output_name))
    return output_name

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])

def utm_window(center, dimension=3):
    """
    Args:
        center(list): Coordinate pair (in UTM integers)
        dimension(float, optional): An odd integer which defines the sample window
            square size (eg 3^2). Defaults to 3

    Returns:
        list: List contians coordinate pair lists.

    Accepts pair of UTM coords (list).
    Return the surrounding 8 coords in a 3x3 pixel window  plus the given "center"
    As list of list objects
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    result = []

    if dimension == 1:
        result.append(center)
    # proceed if dim is odd
    elif dimension %2 != 0:
        half_dim = dimension -1 /2
        # reorient to top left cell to build gridpoints
        start = []
        for (x, y) in center:
            #print x,y
            start.append(x - (30 *half_dim))
            start.append(y - (30 *half_dim))
        # wrap it so we can iterate
        start =[start]
        #print ('start = {}/{}'.format(start, type(start)))

        # iterate from start postition
        for i in xrange(dimension):
            for j in xrange(dimension):
                for (x, y) in start:
                    result.append([x +30 *i, y +30 *j])
    else:
        log.warning('dimension value invalid (not odd integer): {}'.format(dimension))

    return result

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
#import arcpy

def make_pointfile(points, out_name):
    """
    Accept a list of points and a name.
    Make a shapefile containing those points named name, return the name.
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    #log.debug('with {}'.format(points))
    #log.debug('window coords: {}'.format(window_coords))
    result =[]

    pt = arcpy.Point()
    ptGeoms = []
    for p in points:
        #log.debug('working with {}'.format(p))
        pt.X = p[0]
        pt.Y = p[1]
        pg = arcpy.PointGeometry(pt)
        ptGeoms.append(pg)
    final_name = out_name + ".shp"
    arcpy.CopyFeatures_management(ptGeoms, final_name)
    del ptGeoms

    return final_name

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
from variables import *
import arcpy
from arcpy.sa import *
#import utm_window
#import make_pointfile

def select_values(*args):
    """
    (folder_path, corrected_scenes, cloud)
    Get values from points around sample sites, if masks say its ok.
    Return as a table in Proccessed dir?
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    #log.debug('with {}'.format(args))
    folder_path = args[0]
    #need joined to path corrected_scenes = args[1]
    corrected_scenes = [os.path.join(folder_path, i) for i in sorted(args[1])]
    # now that they are pathed, append not_cloud mask in
    corrected_scenes.append(args[2])
    log.debug('with: {}\n{}'.format(folder_path, corrected_scenes))
    result = []

    extract_dir = os.path.join(working_dir, settings['Extract Path'], os.path.basename(folder_path))
    log.debug('extract dir name:{}'.format(extract_dir))

    # Get the sample point shapefile out of the dir corresponding with scene id - subid
    selected_clip_dir = os.path.basename(folder_path).partition('-')[0]
    point_file = os.path.join(settings['Point Path'], (selected_clip_dir + settings['Point Suffix']))
    log.debug('selecting sample points from dir: {}'.format(point_file))
    # have SR ready for features made
    spatial_reference = arcpy.Describe(point_file).spatialReference

    #coordinates = []
    #fields = ['SITE_ID', 'LATITUDE', 'LONGITUDE']
    fields = ['SITE_ID', 'X', 'Y']
    # make sure there's things
    count = int(arcpy.GetCount_management(point_file).getOutput(0))
    log.debug('len of feature {}: {}'.format(point_file, count))
    if count:
        # list of files to aggregate after cursor
        extract_list = []

        # make output dir
        out = extract_dir
        if not os.path.exists(out):
            os.makedirs(out)
            log.debug('Created Extract Dir: {}'.format(out))

        # use sample on the XX_pnt file to get cell address
        xy_table = (os.path.join(extract_dir, 'xy.dbf'))
        # Sample (in_rasters, in_location_data, out_table, {resampling_type})
# this Sample process has a 10 digit limit to field names... LS ids are 12 long
        Sample(corrected_scenes[0], point_file, xy_table)
        # join the SITE_ID from original file to xy_table
        #JoinField_management (in_data, in_field, join_table, join_field, {fields})
        # because of the 10 len field, this borks onsecond parameter
        arcpy.JoinField_management(xy_table, selected_clip_dir[:10], point_file, 'FID', 'SITE_ID')
        # iterate by site_id
        with arcpy.da.SearchCursor(xy_table, fields) as cursor:
            for row in cursor:
                #print row
                log.debug('starting {}, {}/{}'.format(row[0], row[1], row[2]))
                # validate shortens, we just need dash out of name
                clean_siteid = row[0].replace('-', '_').strip()
                # get points, make into window coords
                points = []
                points.append([row[1], row[2]])
                window_coords = utm_window(points, settings['Sample Window Size'])
                log.debug('window coords: {}'.format(window_coords))

                # final extract table name
                output_name = os.path.join(extract_dir, clean_siteid.partition('_')[2] +'.dbf')
                extract_list.append(output_name)
                name = os.path.join(extract_dir, clean_siteid)
                log.debug('extract name: {}, pointfile name: {}'.format(output_name, name))
                # pointfile of window points
                pf_name = make_pointfile(window_coords, name)
                # apply SR from source file
                arcpy.DefineProjection_management(pf_name, spatial_reference)

                # Set local variables
                inFeatures = pf_name
                fieldName1 = "SID"
                fieldType = 'TEXT'
                fieldLength = 20

                # Sample
                Sample(corrected_scenes, pf_name, output_name)
                log.debug('sampled {}'.format(output_name))
                arcpy.AddField_management(output_name, fieldName1, fieldType, \
                    '#', '#', fieldLength)
                log.debug('just added field {}, goign to populate it with {}' \
                    .format(fieldName1, clean_siteid))
                with arcpy.da.UpdateCursor(output_name, '*') as cur:
                    for row in cur:
                        # should be last one since most recently appended
                        row[-1] = clean_siteid
                        cur.updateRow(row)
# sample as script is returing all 0's, and short band names
# !!! 0 = no data out of sample!~!!
# sample in arc returns Null, and full band names..

        # aggregated sample file
        agg_ext = os.path.join(extract_dir, 'agg.dbf')
        # merge data into single file
        arcpy.Merge_management(extract_list, agg_ext)
        log.debug('merged all of {}'.format(clean_siteid.partition('_')[0]))
    else:
        log.debug('{} was empty'.format(point_file))

    return result

import os
import logging
import inspect
module_logger = logging.getLogger(os.path.basename(inspect.stack()[1][1]).split('.')[0])
from variables import *

try:
    import arcpy
except ImportError:
    print "You need arcpy installed"
    sys.exit ( -1 )
from arcpy import env

import os
from os import listdir
from os.path import isfile, join, isdir
import csv
import logging
import inspect

def aggregator(arg):
    """
    Accepts output dir and aggrated file name.
    Reads them all and puts them into a single CSV in the Extracts dir
    Let Recombobulator filter out sites, so that it can be rerun at various thresholds
    """
    log = module_logger.getChild(inspect.currentframe().f_code.co_name)
    log.info(u'Initializing {}'.format(inspect.currentframe().f_code.co_name))
    log.debug('with {} (self-reffering)'.format(arg))
    result = []

    # get folders in the Extract dir
    folders_in_dir = [i for i in listdir(settings['Extract Path']) if isdir(os.path.join(settings['Extract Path'], i))]
    log.debug('folders_in_dir len & (short) list: {} {}'.format(len(folders_in_dir), folders_in_dir[:3]))

    # initialize output
    output = []

    # walk dirs and open the file
    for f in folders_in_dir:
        log.debug('iterating with {}'.format(f))
        f_path = os.path.join(settings['Extract Path'], f, settings['Agg File'])

# would like to pick out some field headings here, and just read those in cursor

        # if first folder, initialize field names
        if output == []:
            field_names = [i.name for i in arcpy.ListFields(f_path)]
            field_names.insert(0, 'folder name')
            log.debug('initializing field names: {}'.format( field_names))
            output.append(field_names)

    # first read, append header selection
        try:
            with arcpy.da.SearchCursor(f_path, "*") as cursor:
                for row in cursor:
                   # print row
                    #output.append(row)
                    x = [i for i in row]
                    x.insert(0, f)
                    output.append(x)
        except:
            log.debug('{} did not have a {}'.format(f, settings['Agg File']))

    # write output into source folder
    with open(os.path.join(settings['Extract Path'], settings['Agg Output']), 'wb') as file:
        writer = csv.writer(file)
        writer.writerows(output)
        log.info('output saved as {}\{}'.format(settings['Extract Path'], settings['Agg Output']))

    return result

def main():
    pass

if __name__ == '__main__':
    main()
