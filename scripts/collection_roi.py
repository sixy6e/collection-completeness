#!/usr/bin/env python

from datetime import datetime
import pandas
import geopandas


def match_pass_id(row, pid):
    # eg contains 'LS5-199101' to get 1991 Jan
    return row.str.contains(pid)

def sensor(row):                                   
    return row.split('-')[0]

def dt(row):                                       
    return datetime.strptime(row.split('-')[1], '%Y%m%d')

wrs2_fname = '/short/v10/jps547/data-collection/ga-nominal-scenes/ADGC_v2_Area_of_Interest.shp'
fname = '/short/v10/jps547/data-collection/collection-completeness.h5'
tm_fname = '/short/v10/jps547/data-collection/tm-world-borders/TM_WORLD_BORDERS-0.3.shp'

store = pandas.HDFStore(fname)
oth_df = store['/oth_and_children_products']
wrs_df = geopandas.read_file(wrs2_fname)
wrs_df.rename(columns={'PATH': 'path', 'ROW': 'row'}, inplace=True)

oth_df = pandas.merge(oth_df, wrs_df, on=['path', 'row'])

oth_df.insert(8, 'sensor', oth_df['pass_id'].apply(sensor))
oth_df.insert(9, 'date', oth_df['pass_id'].apply(dt))

# there are records that will be reporting the same info for given
# columns, i.e. L0_fail, L0_success, L1_fail/success, L1_L(G/Gt/T)
# but we need to sum the nbar(t) and pq records
# so sum the nbar(t) pq records, and join back with the records
# from the original dataframe

# blank dataframe to contain children products
cols = ['pass_name', 'nbar_exists', 'nbart_exists', 'pq_exists']
children = pandas.DataFrame(columns=cols)
cols.remove('pass_name')

groups = oth_df.groupby('pass_name')
for name, group in groups:
    res = group[cols].sum()
    res['pass_name'] = name
    children = children.append(res, ignore_index=True)

children.rename(columns={'nbar_exists': 'nbar',
                         'nbart_exists': 'nbart',
                         'pq_exists': 'pq'}, inplace=True)

merged = pandas.merge(oth_df, children, on=['pass_name'])

df2 = merged.drop_duplicates('pass_name')
cols = ['sensor',
        'date',
        'L0_fail',
        'L0_success',
        'L1_fail',
        'L1_success',
        'L1_L1Gt',
        'L1_L1T',
        'nbar',
        'nbart',
        'pq']
df3 = df2[cols]

df3.set_index('date', inplace=True)

groups = df3.groupby('sensor')
store3 = pandas.HDFStore('collection-monthly-counts-roi.h5', 'w', complib='blosc')
for name, group in groups:
    outdf = group.resample('M', how=sum)

    exp = "L0_completeness = L0_success / (L0_success + L0_fail) * 100"
    outdf.eval(exp)
    exp = "L1_completeness = L1_success / (L1_success + L1_fail) * 100"
    outdf.eval(exp)
    exp = "oth_percent = (L1_L1T + L1_L1Gt) / L1_success  * 100"
    outdf.eval(exp)
    exp = "nbar_completeness = nbar / (L1_L1T + L1_L1Gt) * 100"
    outdf.eval(exp)
    exp = "nbart_completeness = nbart / (L1_L1T + L1_L1Gt) * 100"
    outdf.eval(exp)
    exp = "pq_completeness = pq / (L1_L1T + L1_L1Gt) * 100"
    outdf.eval(exp)
    exp = "pq_completeness_relative = pq / nbar * 100"
    outdf.eval(exp)

    outdf.to_excel(name + '-roi.xls')
    store3[name] = outdf
