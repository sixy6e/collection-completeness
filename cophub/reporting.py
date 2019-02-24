#!/usr/bin/env python

"""
Digest, analyse and report on the Apache and SARA logs provided by NCI.
"""

from oauth2client.service_account import ServiceAccountCredentials
import numpy
import pandas
import pygsheets
import tabulate
import click


template = ("""# SARA Useage Report

## Downloads per Sentinel Collection

{c_downloads}

""")


def apache_log(fname):
    """
    Read, analyse and report against the Apache log.

    Current column names:

        * date
        * ip
        * country
        * project
        * project_name
        * dataset
        * service
        * file_type
        * access_type
        * platform
        * hits
        * traffic_browse
        * traffic_browse_mb
        * traffic_browse_gb
        * traffic_data
        * traffic_data_mb
        * traffic_data_gb
    """
    df = pandas.read_csv(fname)
    df['access_count'] = 1

    # traffic summary (in TB) by service
    summary = df.pivot_table(values='traffic_data', index='service',
                             aggfunc=numpy.sum)
    summary['traffic_data'] = summary['traffic_data'] / 1099511627776

    # drop certain records
    # (TODO check what they refer too, as they have hits, but no data transfer)
    query = ((df.dataset == 'Sentinel-1') | (df.dataset == 'Sentinel-2') |
            (df.dataset == 'Sentinel-3'))
    subs = df[~query]

    # traffic summary in GB by dataset
    datasets_d = subs.pivot_table(values='traffic_data', index='dataset',
                                  aggfunc=numpy.sum)
    datasets_d['traffic_data'] = datasets_d['traffic_data'] / 1024 / 1024 / 1024

    # access count summary per dataset
    datasets_c = subs.pivot_table(values='access_count', index='dataset',
                                  aggfunc=numpy.sum)

    # append access count summary
    datasets_d['access_count'] = datasets_c['access_count']

    # authorisation
    scopes = ['https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/sixy/Downloads/CopHub-fb57e386091f.json', scopes)
    gc = pygsheets.authorize(credentials=credentials)

    # Apache-Logs directory on drive                                                
    sheets = gc.create('Apache-History-201806', parent_id='1PaI4V6YKFlAkNAuUNnDRwVDQMkD4LS8L')
    sheets.sheet1.rows = df.shape[0]                                               
    sheets.sheet1.set_dataframe(df, (1, 1))

    worksheet = sheets.add_worksheet('Summaries')
    worksheet.set_dataframe(summary, (3, 1), copy_index=True, copy_head=True)
    worksheet.cell('A1').value = 'Traffic Downloads in TB by Service'
    worksheet.cell('A3').value = 'Service'

    worksheet.set_dataframe(datasets_d, 'A12', copy_index=True, copy_head=True)
    worksheet.cell('A10').value = 'Traffic Downloads in GB and access counts by dataset'
    worksheet.cell('A12').value = 'Dataset'


def sara_log(fname):
    """
    Read, analyse and report against the SARA log.

    Current column names:

        * gid
        * email
        * method
        * service
        * collection
        * resourceid
        * query
        * querytime
        * url
        * ip
        * productidentifier
    """
    df = pandas.read_csv(fname)

    # handle only the GET requests
    df_get = df[df['method'] == 'GET'].copy()

    # collection summary
    df_get['count'] = 1
    c_summary = df_get.pivot_table(values='count', index='collection',
                                   aggfunc=numpy.sum)

    # authorisation
    scopes = ['https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/sixy/Downloads/CopHub-fb57e386091f.json', scopes)
    gc = pygsheets.authorize(credentials=credentials)

    # google sheets creation
    sheets = gc.create('SARA-History-201807', parent_id='1-YUyrfhKgmgIQct2-7V5DPJrW7vVmocK')
    sheets.sheet1.rows = df.shape[0] # see https://github.com/nithinmurali/pygsheets/issues/124
    sheets.sheet1.set_dataframe(df, (1, 1))
    sheets.sheet1.title = 'SARA-Log'

    worksheet = sheets.add_worksheet('Collection Summary')
    worksheet.set_dataframe(c_summary, (3, 1), copy_index=True, copy_head=True)
    worksheet.cell('A1').value = 'No. of downloads per Sentinel collection'
    worksheet.cell('A3').value = 'Collection'
    worksheet.cell('B3').value = 'Downloads'


@click.command()
@click.option("--sara-fname", type=click.Path(exists=True, readable=True))
@click.option("--apache-fname", type=click.Path(exists=True, readable=True))


def main(sara_fname, apache_fname):
    """
    Main routine.
    """
    sara_log(sara_fname)
    # apache_log(apache_fname)


if __name__ == '__main__':
    main()
