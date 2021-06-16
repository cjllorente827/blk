import yt
yt.enable_parallelism()
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable


from blk import Do_Query

FIELD_CMAPS = {
    'density': 'viridis',
    'temperature': 'plasma',
    'metallicity': 'dusk'}

def main():

    #run_smol_test()

    #run_big_test()

    # plot big dataset from the cache
    big_ds_fname  = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
    density, qtime = Do_Query(ytProjection, big_ds_fname, 'density')

    big_ds_fname  = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
    temperature, qtime = Do_Query(ytProjection, big_ds_fname, 'temperature')

    if yt.is_root():
        plot(density, 'density')
        plot(temperature, 'temperature')

        double_plot([density, temperature], ['density', 'temperature'])
    


def run_smol_test():

    print("Running small test...")

    smol_ds_fname = "/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042"
    
    # do the Query once with a clear cache
    qdata, no_cache_time = Do_Query(ytProjection, smol_ds_fname, 'density', clear_cache=True)

    # do it again but cached this time
    qdata, cache_time = Do_Query(ytProjection, smol_ds_fname, 'density')

    print(f"""
No cache query time : {no_cache_time} seconds
Cached query time   : {cache_time} seconds
""")

    #plot(qdata)

def run_big_test():

    print("Running big test...")

    big_ds_fname  = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"

    # do the Query once with a clear cache
    qdata, no_cache_time = Do_Query(ytProjection, big_ds_fname, 'density', clear_cache=True)

    # do it again but cached this time
    qdata, cache_time = Do_Query(ytProjection, big_ds_fname, 'density')

    print(f"""
No cache query time : {no_cache_time} seconds
Cached query time   : {cache_time} seconds
""")

    #plot(qdata)


def ytProjection(ds_fname, field):

    ds = yt.load(ds_fname)
    p = yt.ProjectionPlot(ds, 'z', field, weight_field='density')
    frb = p.data_source.to_frb(ds.domain_width[0], 1024)
    result = np.array(frb[field])
    return result

def plot(projection_data, field):

    plt.imshow(projection_data, origin='lower', 
            norm=LogNorm(), cmap=FIELD_CMAPS[field])
    cbar = plt.colorbar()
    cbar.set_label(field)
    plt.show()

def double_plot(data_array, field_names):

    fig, axes = plt.subplots(1,2)

    for i,ax in enumerate(axes):
        im = ax.imshow(data_array[i], origin='lower', 
            norm=LogNorm(), cmap=FIELD_CMAPS[field_names[i]])
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="5%", pad=0.05)
        cbar = fig.colorbar(im, cax=cax)
        cbar.set_label(field_names[i])
    plt.show()

if __name__ == "__main__":
    main()