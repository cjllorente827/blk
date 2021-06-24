

import gc, yt
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

import proto_movie

def main():
    fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'
    nframes = 150

    proto_movie.Movie("test_proto_movie", fname, nframes, max_procs=4)


def Query(ds_fname, frame_start, frame_end):

    ds = yt.load(ds_fname)
    nframes = frame_end - frame_start

    L = ds.domain_width[0].to('Mpc/h')
    dL = (ds.quan(0.5, 'Mpc/h') / L ).value
    vel = (1.0-dL)/nframes
    frame_data = np.zeros((nframes, 800, 800))

    ranges = zip(range(frame_start, frame_end), range(nframes))

    gc.disable()
    
    for i, j in ranges:
        next_slab = ds.r[:,:,i*vel : i*vel+dL]
        plot = yt.ProjectionPlot(ds, 'z', 'density', data_source=next_slab, weight_field='density')

        frame = np.array(plot.frb['density'])

        frame_data[j][:] = frame[:]

        del frame
        del next_slab
        del plot

        gc.collect()

    gc.enable()
    return frame_data

def Analysis(data):
    print("Stub method for Analysis")

def Plot(data, index):
    fig, ax = plt.subplots(1,1)

    im = ax.imshow(data, origin='lower', 
        norm=LogNorm(), cmap='viridis',
        vmin=1e-32, vmax=1e-27)

    cbar = fig.colorbar(im)
    cbar.set_label("Density")
    
    def output_fname(n):
        return f'tmp/tmp_{n:04d}.png'

    plt.savefig(output_fname(index))
    plt.close()


if __name__ == "__main__":
    main()