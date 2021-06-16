import yt
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm



from proto_query import Do_Query

FIELD_CMAPS = {
    'density': 'viridis',
    'temperature': 'plasma',
    'metallicity': 'dusk'}

def main():

    #run_smol_test()

    #run_big_test()
    
    # plot big dataset from the cache
    big_ds_fname  = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
    qdata, qtime = Do_Query(Query, big_ds_fname)
    plot(qdata, 'density')
    


def run_smol_test():

    print("Running small test...")

    smol_ds_fname = "/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042"
    
    # do the Query once with a clear cache
    qdata, no_cache_time = Do_Query(Query, smol_ds_fname, clear_cache=True)

    # do it again but cached this time
    qdata, cache_time = Do_Query(Query, smol_ds_fname)

    print(f"""
No cache query time : {no_cache_time}
Cached query time   : {cache_time}
""")

    #plot(qdata)

def run_big_test():

    print("Running big test...")

    big_ds_fname  = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"

    # do the Query once with a clear cache
    qdata, no_cache_time = Do_Query(Query, big_ds_fname, clear_cache=True)

    # do it again but cached this time
    qdata, cache_time = Do_Query(Query, big_ds_fname)

    print(f"""
No cache query time : {no_cache_time}
Cached query time   : {cache_time}
""")

    #plot(qdata)



# Query should take ONlY a dataset filename as an argument
# and then return some serializable object that represents
# the desired result of that query
#
# Later on, I can add the argument list onto the target string
# so that all arguments passed to the query function get 
# saved in the hash
def Query(ds_fname):

    ds = yt.load(ds_fname)

    field = 'density'

    plot = yt.ProjectionPlot(ds, 'z', field, weight_field='density')
    frb = plot.data_source.to_frb(ds.domain_width[0], 800)
    result = np.array(frb[field])
    return result

def plot(projection_data, field):

    plt.imshow(projection_data, origin='lower', 
            norm=LogNorm(), cmap=FIELD_CMAPS[field])
    cbar = plt.colorbar()
    cbar.set_label(field)
    plt.show()

if __name__ == "__main__":
    main()