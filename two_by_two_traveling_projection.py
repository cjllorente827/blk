

import blk
import numpy as np
import yt, time, sys
import datetime
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
from mpi4py import MPI


OUT_DIR = "/mnt/home/llorente/blk"

HEIGHT = 15
WIDTH = 18

IMG_RES = 1024

FIELDS = [
    'density', 
    'metallicity',
    'temperature', 
    ('deposit', 'stars_density')]

CMAPS = [
    'viridis', 
    'dusk', 
    'plasma', 
    'Purples_r']

TITLES = [
    'Density',
    'Metallicity', 
    'Temperature', 
    'Stellar Density']


#fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'
#out_fname = 'two_by_two_test_z_0.mp4'
fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
out_fname = 'two_by_two_25Mpc_z_0.mp4'
#fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0135/RD0135"
#out_fname = 'two_by_two_50Mpc_z_0.mp4'
TOTAL_FRAMES = 300

QUERY_ARGS = [fname, FIELDS, TOTAL_FRAMES, IMG_RES]
PLOT_ARGS = []

class TwoByTwoFrame:
    """
    Class that stores data required to render single frame of the traveling projection
    animation. It's two-by-two, meaning that it stores four 2D numpy arrays of 
    a set (square) resolution. A frame knows its place in the frame order, i.e its index. 
    The data_dict is a dictionary which stores the fixed resolution buffer data
    with the field names acting as keys. 
    """

    def __init__(self, index, field_names, resolution):
        self.index = index
        self.field_names = field_names
        
        resolution_tup = (resolution, resolution)
        self.resolution = resolution_tup
        self.data_dict = {
            field_names[0] : np.zeros( resolution_tup ),
            field_names[1] : np.zeros( resolution_tup ),
            field_names[2] : np.zeros( resolution_tup ),
            field_names[3] : np.zeros( resolution_tup )
        }

    def __getitem__(self, key):
        return self.data_dict[key]

    def __setitem__(self, key, nparray):
        self.data_dict[key][:] = nparray[:]

    def __repr__(self):
        return f"""
A TwoByTwoFrame at {self.resolution[0]}x{self.resolution[1]} resolution with fields:
{self.field_names[0]}
{self.field_names[1]}
{self.field_names[2]}
{self.field_names[3]}
"""

def Query(ds_fname, fields, total_frames, img_res):

    # get important info for running in parallel
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    yt.funcs.mylog.setLevel(50)

    # set up yt's particle filter
    def stars(pfilter, data):
        filter = data[("all", "particle_type")] == 2 # DM = 1, Stars = 2
        return filter

    yt.add_particle_filter("stars", function=stars, filtered_type='all', requires=["particle_type"])


    # load the dataset and add a particle filter
    ds = yt.load(ds_fname)
    ds.add_particle_filter("stars")

    # figure out which frames we are responsible for
    frame_start = int(rank/size * total_frames)
    frame_end = int ( (rank+1)/size * total_frames )

    # Do math to figure out how our slab travels through the box
    L = ds.domain_width[0].to('Mpc/h')
    dL = (ds.quan(0.2, 'Mpc/h') / L ).value
    vel = (1.0-dL)/total_frames

    # the number of frames that this process is creating
    # total_frames represents ALL the frames that will 
    # be created
    nframes = frame_end - frame_start

    frame_data = []
    
    # We need two sets of indices: 
    # one that starts at f_start and ends at f_end -- fsi (frame start index)
    # and the other that starts at zero and ends at f_end-f_start -- zsi (zero start index)
    #
    # the data array must be indexed with the zero start index
    # creation of the slab must use the frame start index
    indexes = zip(range(frame_start, frame_end), range(nframes))

    for fsi,zsi in indexes:
        next_slab = ds.r[:,:,fsi*vel : fsi*vel+dL]
        plot = yt.ProjectionPlot(ds, 'z', fields, 
            data_source=next_slab, 
            weight_field='density', 
            buff_size=(img_res, img_res))

        # create the frame object to handle the data
        frame = TwoByTwoFrame(fsi, TITLES, img_res)

        # handle the first three fields generically
        for i in range(3):
            frame[TITLES[i]] = np.array(plot.frb[fields[i]])

        # stellar density requires a floor, so we swap it
        # out manually here
        # could also possibly do this in the plot code
        star_density = np.array(plot.frb[('deposit','stars_density')])
        star_density[star_density == 0.] = 1e-33

        frame[TITLES[3]] = star_density
        
        frame_data.append(frame)

    return frame_data

def Analysis(data):
    print("Stub method for Analysis")

def Plot(data):

    for i, frame in enumerate(data):

        axes = [None]*4
        fig, ((axes[0], axes[1]),(axes[2], axes[3])) = plt.subplots(
            2,2,
            figsize=(WIDTH, HEIGHT),
            sharex='all',
            sharey='all')

        plt.subplots_adjust(left=0.03, \
                            bottom=0.05,\
                            right=0.9,\
                            top=0.95,\
                            wspace=0.1,\
                            hspace=0.01)

        imgs = []
        for i,ax in enumerate(axes):
            im = ax.imshow(np.abs(frame[TITLES[i]]), 
            origin='lower', 
            cmap=CMAPS[i], 
            norm=LogNorm())

            divider = make_axes_locatable(ax)
            cax = divider.append_axes("right", size="5%", pad=0.05)
            cbar = fig.colorbar(im, cax=cax)
            cbar.set_label(TITLES[i])

        # def output_fname(n):
        #     return f'{OUT_DIR}/tmp/tmp_{n:04d}.png'

        output_fname = lambda n: f'{OUT_DIR}/tmp/tmp_{n:04d}.png'

        out_file = output_fname(frame.index)
        print(f"Saving to file: {out_file}")
        plt.savefig(out_file)
        plt.close()
