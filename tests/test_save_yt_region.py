

import yt
import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

yt.funcs.mylog.setLevel(50)

import blk

OUT_DIR = "/mnt/home/llorente/blk/tests/test_results"
TEST_DATASET = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'

def test_multi():

    box, miss_time = blk.Do_Query(EntireBoxMultiField, TEST_DATASET, ["density", "temperature", "cell_mass"], clear_cache=True )

    plot = yt.PhasePlot(box, "density", "temperature","cell_mass")

    plot.set_unit('cell_mass', 'Msun')

    plot.save("cache_miss")

    box, hit_time = blk.Do_Query(EntireBoxMultiField, TEST_DATASET, ["density", "temperature", "cell_mass"], clear_cache=False)

    plot = yt.PhasePlot(box, "density", "temperature","cell_mass")

    plot.set_unit('cell_mass', 'Msun')

    plot.save("cache_hit")

    print(f"""
Cache miss time : {miss_time}    
Cache hit time  : {hit_time}    
""")


def EntireBoxSingleField(ds_fname, field):

    ds = yt.load(ds_fname)
    region = ds.all_data()
    result = region[field]
    
    return region

def EntireBoxMultiField(ds_fname, fields):

    ds = yt.load(ds_fname)
    region = ds.all_data()
    for f in fields:
        result = region[f]
    
    return region

if __name__ == "__main__":
    test_multi()