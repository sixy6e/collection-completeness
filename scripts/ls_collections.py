#!/usr/bin/env python

import datetime
import os
from os.path import join as pjoin, basename, dirname, exists
import re
import xml.etree.ElementTree as ET
import argparse
from mpi4py import MPI
import pandas
from eotools.tiling import scatter


BASE_DIR = "/g/data/v10/reprocess/{sensor}/level1"
SENSORS = ['ls5', 'ls7', 'ls8']

L1T_PATTERN = ('(?P<spacecraft_id>LS\d)_(?P<sensor_id>\w+)_'
               '(?P<product_type>\w+)'
               '_(?P<product_id>P\d+.*)_GA(?P<product_code>.*)-'
               '(?P<station_id>\d+)_'
               '(?P<wrs_path>\d+)_(?P<wrs_row>\d+)_'
               '(?P<acquisition_date>\d{8})')
PAT = re.compile(L1T_PATTERN)

NBAR_BASE = '/g/data/rs0/scenes/nbar-scenes-tmp/{sensor}/{year}/{month}/output/nbar/{scene}/ga-metadata.yaml'
NBART_BASE = '/g/data/rs0/scenes/nbar-scenes-tmp/{sensor}/{year}/{month}/output/nbart/{scene}/ga-metadata.yaml'
PQ_BASE = '/g/data/rs0/scenes/pq-scenes-tmp/{sensor}/{year}/{month}/output/pqa/{scene}/ga-metadata.yaml'

WRS2SHAPEFILE = '/g/data2/v10/public/agdcv2_completeness/reference/wrs2_descending.shp'


def match_pass_id(row, pid):
    # eg contains 'LS5-199101' to get 1991 Jan
    return row.str.contains(pid)


def sensor(row):                                   
    return row.split('-')[0]


def dt(row):                                       
    return datetime.datetime.strptime(row.split('-')[1], '%Y%m%d')


def nbar_name_from_l1t(l1t_fname):
    """
    Return an NBAR file name given a L1T file name or None if
    invalid L1T name.
    """
    m = PAT.match(basename(l1t_fname))
    fmt = "{}_{}_NBAR_P54_GANBAR01-{}_{}_{}_{}"
    if m:
        scene = fmt.format(m.group('spacecraft_id'), m.group('sensor_id'),
                           m.group('station_id'), m.group('wrs_path'),
                           m.group('wrs_row'), m.group('acquisition_date'))
        dt = datetime.datetime.strptime(m.group('acquisition_date'), "%Y%m%d")
        year = dt.year
        month = '{0:02d}'.format(dt.month)

        return NBAR_BASE.format(sensor=m.group('spacecraft_id').lower(),
                                year=year, month=month, scene=scene)


def nbart_name_from_l1t(l1t_fname):
    """
    Return an NBAR file name given a L1T file name or None if
    invalid L1T name.
    """
    m = PAT.match(basename(l1t_fname))
    fmt = "{}_{}_NBART_P54_GANBART01-{}_{}_{}_{}"
    if m:
        scene = fmt.format(m.group('spacecraft_id'), m.group('sensor_id'),
                           m.group('station_id'), m.group('wrs_path'),
                           m.group('wrs_row'), m.group('acquisition_date'))
        dt = datetime.datetime.strptime(m.group('acquisition_date'), "%Y%m%d")
        year = dt.year
        month = '{0:02d}'.format(dt.month)

        return NBART_BASE.format(sensor=m.group('spacecraft_id').lower(),
                                 year=year, month=month, scene=scene)


def pqa_name_from_l1t(l1t_fname):
    """
    Return a PQA file name given a L1T file name or None if
    invalid L1T name.
    """
    m = PAT.match(basename(l1t_fname))
    fmt = "{}_{}_PQ_P55_GAPQ01-{}_{}_{}_{}"
    if m:
        scene = fmt.format(m.group('spacecraft_id'), m.group('sensor_id'),
                           m.group('station_id'), m.group('wrs_path'),
                           m.group('wrs_row'), m.group('acquisition_date'))
        dt = datetime.datetime.strptime(m.group('acquisition_date'), "%Y%m%d")
        year = dt.year
        month = '{0:02d}'.format(dt.month)

        return PQ_BASE.format(sensor=m.group('spacecraft_id').lower(),
                              year=year, month=month, scene=scene)


def pqa_name_from_nbar(nbar_fname):
    """
    Return a PQ file name given a NBAR file name or None if
    invalid NBAR name.

    A helper function, simply wraps `pqa_name_from_l1t`.
    """
    return pqa_name_from_l1t(nbar_fname)



def process_lpgs_log(xml_fname):
    """
    This is ugly. There must be a better way than this to retrieve
    data from an xml.
    """
    data = {}
    level1_name = dirname(dirname(xml_fname))
    path, row = [int(i) for i in level1_name.split('_')[5:7]]
    data['level1_name'] = level1_name
    data['path'] = path
    data['row'] = row

    tree = ET.parse(xml_fname)
    root = tree.getroot()

    # retrieve the first level of the tree
    result = {}
    for child in root:
        result[child.tag] = child.attrib

    l0_keys = ['success', 'fail']
    l1_keys = ['success', 'fail', 'L1G', 'L1Gt', 'L1T']

    pass_id = result['LandsatProcessingRequest']['id']
    data['pass_id'] = pass_id

    l0_data = result['L0RpProcessing']
    l1_data = result['L1Processing']

    for key in l0_keys:
        data['L0_' + key] = int(l0_data[key])

    for key in l1_keys:
        data['L1_' + key] = int(l1_data[key])

    # explore the leaves of the 'LandsatProcessingRequest' branch
    find = root.find('LandsatProcessingRequest')
    result = {}
    for child in find.iter():
        result[child.tag] = child.text

    data['pass_name'] = basename(dirname(dirname(result['WorkingFolder'])))

    return data


def main_mpi(input_fname):
    with open(input_fname) as src:
        files = src.readlines()

    files = [f.strip() for f in files]

    # processor info
    rank = MPI.COMM_WORLD.rank
    n_proc = MPI.COMM_WORLD.size

    columns = ['L1_L1Gt',
               'L1_success',
               'L0_fail',
               'L1_L1T',
               'L0_success',
               'level1_name',
               'pass_name',
               'L1_fail',
               'L1_L1G']
    df = pandas.DataFrame(columns=columns)

    # assign each processor a block to work on
    rank_list = scatter(files, n_proc)[rank]
    # rank_list = scatter(files[0:64], n_proc)[rank]

    failures = []
    packagetmp = []
    for fname in rank_list:
        if "failure" in fname:
            failures.append(fname)
            continue
        if "packagetmp" in fname:
            packagetmp.append(fname)
            continue
        result = process_lpgs_log(fname)
        df = df.append(result, ignore_index=True)

    # seperate the sys and oth products and failed
    # wh = df['level1_name'].str.contains("failure")
    # fails = df[wh].copy()
    # df = df[~wh]
    wh = df['level1_name'].str.contains("SYS")
    sys_df = df[wh].copy()
    oth_df = df[~wh].copy()

    # predict nbar, nbart, pq scene names
    oth_df['nbar_name'] = oth_df['level1_name'].apply(nbar_name_from_l1t)
    oth_df['nbart_name'] = oth_df['level1_name'].apply(nbart_name_from_l1t)
    oth_df['pq_name'] = oth_df['level1_name'].apply(pqa_name_from_nbar)

    oth_df['sensor'] = oth_df['pass_id'].apply(sensor)
    oth_df['date'] = oth_df['pass_id'].apply(dt)
    sys_df['sensor'] = sys_df['pass_id'].apply(sensor)
    sys_df['date'] = sys_df['pass_id'].apply(dt)


    # TODO: under MPI the results appear to be all False
    # once the results have been combined, then do the exists function

    # determine whether or not a child product exists
    # oth_df['nbar_exists'] = oth_df['nbar_name'].apply(exists)
    # oth_df['nbart_exists'] = oth_df['nbart_name'].apply(exists)
    # oth_df['pq_exists'] = oth_df['pq_name'].apply(exists)

    # output
    out_fname = 'collection-completeness-{rank}.h5'.format(rank=rank)
    store = pandas.HDFStore(out_fname, 'w', complib='blosc')
    store['lpgs_fails'] = pandas.DataFrame({'level0_fname': failures})
    store['packagetmp'] = pandas.DataFrame({'packagetmp': packagetmp})
    store['sys_products'] = sys_df
    store['oth_and_children_products'] = oth_df
    store.close()


def combine(ncpus):

    out_fname = 'collection-completeness.h5'
    collection_fmt = 'collection-completeness-{}.h5'
    keys = ['sys_products', 'oth_and_children_products']

    results = {}
    for key in keys:
        results[key] = pandas.DataFrame()

    store = pandas.HDFStore(out_fname, 'w', complib='blosc')

    for i in range(ncpus):
        tmp_store = pandas.HDFStore(collection_fmt.format(i), 'r')
        for key in keys:
            results[key] = results[key].append(tmp_store[key],
                                               ignore_index=True)
        tmp_store.close()

    # apply here as for some reason it isn't working under mpi
    # determine whether or not a child product exists
    df = results['oth_and_children_products']
    df['nbar_exists'] = df['nbar_name'].apply(exists)
    df['nbart_exists'] = df['nbart_name'].apply(exists)
    df['pq_exists'] = df['pq_name'].apply(exists)

    results['oth_and_children_products'] = df

    for key in keys:
        store[key] = results[key]
    store.close()


def main():
    columns = ['L1_L1Gt',
               'L1_success',
               'L0_fail',
               'L1_L1T',
               'L0_success',
               'level1_name',
               'pass_name',
               'L1_fail',
               'L1_L1G']
    df = pandas.DataFrame(columns=columns)

    # this could take a while...
    for sensor in SENSORS:
        for root, dirs, files in os.walk(BASE_DIR.format(sensor=sensor)):
            for fname in files:
                if fname == 'lpgs_out.xml':
                    result = process_lpgs_log(pjoin(root, fname))
                    df = df.append(result, ignore_index=True)

    # seperate the sys and oth products
    wh = df['level1_name'].str.contains("SYS")
    sys_df = df[wh].copy()
    oth_df = df[~wh].copy()

    # predict nbar, nbart, pq scene names
    oth_df['nbar_name'] = oth_df['level1_name'].apply(nbar_name_from_l1t)
    oth_df['nbart_name'] = oth_df['level1_name'].apply(nbart_name_from_l1t)
    oth_df['pq_name'] = oth_df['level1_name'].apply(pqa_name_from_nbar)

    # determine whether or not a child product exists
    oth_df['nbar_exists'] = oth_df['nbar_name'].apply(exists)
    oth_df['nbart_exists'] = oth_df['nbart_name'].apply(exists)
    oth_df['pq_exists'] = oth_df['pq_name'].apply(exists)

    # output
    store = pandas.HDFStore('collection-completeness.h5', 'w', complib='blosc')
    store['sys_products'] = sys_df
    store['oth_and_children_products'] = oth_df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="GA Landsat Harvest")
    parser.add_argument('--combine', action="store_true",
                        help=("If set, then the tables stored in seperate "
                              "fileswill be combined into single file."))
    parser.add_argument('--ncpus', type=int, default=16,
                        help=("The number of CPU's used to harvest the "
                              "Landsat colletion. Default is 16"))

    parsed_args = parser.parse_args()

    if parsed_args.combine:
        combine(parsed_args.ncpus)
    else:
        files_fname = 'ls578-lpgs_out.xml.txt'
        main_mpi(files_fname)
