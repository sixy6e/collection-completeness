#!/usr/bin/env python

from datetime import datetime
import matplotlib
matplotlib.use('Agg')
from matplotlib.backends.backend_pdf import PdfPages
import pandas
import geopandas


def match_pass_id(row, pid):
    # eg contains 'LS5-199101' to get 1991 Jan
    return row.str.contains(pid)

def sensor(row):                                   
    return row.split('-')[0]

def dt(row):                                       
    return datetime.strptime(row.split('-')[1], '%Y%m%d')

wrs2_fname = 'wrs2-descending/wrs2_descending.shp'
fname = 'collection-completeness.h5'
tm_fname = 'tm-world-borders/TM_WORLD_BORDERS-0.3.shp'

store = pandas.HDFStore(fname)
oth_df = store['/oth_and_children_products']
sys_df = store['/sys_products']
wrs_df = geopandas.read_file(wrs2_fname)
wrs_df.rename(columns={'PATH': 'path', 'ROW': 'row'}, inplace=True)

oth_df = pandas.merge(oth_df, wrs_df, on=['path', 'row'])
sys_df = pandas.merge(sys_df, wrs_df, on=['path', 'row'])

store2 = pandas.HDFStore('collection-merge2.h5', 'w', complib='blosc')
store2['oth_merge'] = oth_df
store2['sys_merge'] = sys_df

oth_groups = oth_df.groupby('pass_id')
sys_groups = sys_df.groupby('pass_id')

oth_wh = oth_groups.pass_id.apply(match_pass_id, 'LS5-199101')
sys_wh = sys_groups.pass_id.apply(match_pass_id, 'LS5-199101')

oth_subs_gdf = geopandas.GeoDataFrame(oth_df[oth_wh])
sys_subs_gdf = geopandas.GeoDataFrame(sys_df[sys_wh])

tm = geopandas.read_file(tm_fname)
aus = tm[tm['NAME'] == 'Australia']

oth_grps = oth_subs_gdf.groupby('pass_id')
sys_grps = sys_subs_gdf.groupby('pass_id')

# with PdfPages('ls5-1991-Jan-pass.pdf') as pdf:
#     for name, grp in oth_grps:
#         ax = aus.plot()
#         ax.set_title(name)
#         grp.plot('path', ax=ax)
#         try:
#             sys_grps.get_group(name).plot('path', edgecolor='red', ax=ax)
#         except KeyError:
#             pass
#         pdf.savefig()

# monthly reports, scene counts from which you can derive expected counts
oth = store['/oth_and_children_products']
sys = store['/sys_products']

# we'll append sys data, so insert cols of correct value
cols = ['nbar_exists', 'nbart_exists', 'pq_exists']
for col in cols:
    sys[col] = False

cols = ['nbar_name', 'nbart_name', 'pq_name']
for col in cols:
    sys[col] = ''

# append
df = pandas.concat([oth, sys], keys=['oth', 'sys'])

df.insert(8, 'sensor', df['pass_id'].apply(sensor))
df.insert(9, 'date', df['pass_id'].apply(dt))

# blank dataframe to contain children products
cols = ['pass_name', 'nbar_exists', 'nbart_exists', 'pq_exists']
children = pandas.DataFrame(columns=cols)
cols.remove('pass_name')

groups = df.groupby('pass_name')
for name, group in groups:
    res = group[cols].sum()
    res['pass_name'] = name
    children = children.append(res, ignore_index=True)

children.rename(columns={'nbar_exists': 'nbar',
                         'nbart_exists': 'nbart',
                         'pq_exists': 'pq'}, inplace=True)

merged = pandas.merge(df, children, on=['pass_name'])

df2 = merged.drop_duplicates('pass_name')
cols = ['sensor',
        'date',
        'L0_fail',
        'L0_success',
        'L1_fail',
        'L1_success',
        'L1_L1G',
        'L1_L1Gt',
        'L1_L1T',
        'nbar',
        'nbart',
        'pq']
df3 = df2[cols]

df3.set_index('date', inplace=True)

groups = df3.groupby('sensor')
store3 = pandas.HDFStore('collection-monthly-counts.h5', 'w', complib='blosc')
for name, group in groups:
    outdf = group.resample('M', how=sum)

    exp = "L0_completeness = L0_success / (L0_success + L0_fail) * 100"
    outdf.eval(exp)
    exp = "L1_completeness = L1_success / (L1_success + L1_fail) * 100"
    outdf.eval(exp)
    exp = "nbar_completeness = nbar / (L1_L1T + L1_L1Gt) * 100"
    outdf.eval(exp)
    exp = "nbart_completeness = nbart / (L1_L1T + L1_L1Gt) * 100"
    outdf.eval(exp)
    exp = "pq_completeness = pq / nbar * 100"
    outdf.eval(exp)

    outdf.to_excel(name + '.xls')
    store3[name] = outdf
