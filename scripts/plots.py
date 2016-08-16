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

tm_fname = 'tm-world-borders/TM_WORLD_BORDERS-0.3.shp'

store = pandas.HDFStore('collection-merge2.h5', 'r')
oth_df = store['oth_merge']
sys_df = store['sys_merge']

oth_groups = oth_df.groupby('pass_id')
sys_groups = sys_df.groupby('pass_id')

oth_wh = oth_groups.pass_id.apply(match_pass_id, 'LS8-201604')
sys_wh = sys_groups.pass_id.apply(match_pass_id, 'LS8-201604')

oth_subs_gdf = geopandas.GeoDataFrame(oth_df[oth_wh])
sys_subs_gdf = geopandas.GeoDataFrame(sys_df[sys_wh])

tm = geopandas.read_file(tm_fname)
aus = tm[tm['NAME'] == 'Australia']

oth_grps = oth_subs_gdf.groupby('pass_id')
sys_grps = sys_subs_gdf.groupby('pass_id')

with PdfPages('ls8-2016-Apr-pass.pdf') as pdf:
    for name, grp in oth_grps:
        ax = aus.plot()
        ax.set_title(name)
        #grp.plot('path', ax=ax)
        grp.plot('path')
        #try:
        #    sys_grps.get_group(name).plot('path', edgecolor='red', ax=ax)
        #except KeyError:
        #    pass
        pdf.savefig()

