


import numpy as np
import yt, os, subprocess
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
from mpi4py import MPI

plt.rcParams.update({'font.size': 20})

OUT_DIR = "/mnt/home/llorente/blk"
TEMP_DIR = f"{OUT_DIR}/temp"

# Produces a 2880x2160 image
DPI = 96
HEIGHT = (4.5*480)//DPI
WIDTH  = (4.5*640)//DPI

IMG_RES = 1024


#fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'
#out_fname = 'two_by_two_test_z_0.mp4'

####################################################################
# 25 Mpc outputs
####################################################################
#COMOVING_BOX_SIZE = 25
#fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
#REDSHIFT = 0

#fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0166/RD0166"
#REDSHIFT = 1

# fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0111/RD0111"
# REDSHIFT = 2


####################################################################
# 50 Mpc outputs
####################################################################
COMOVING_BOX_SIZE = 50
# fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0135/RD0135"
# REDSHIFT = 0

# fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0088/RD0088"
# REDSHIFT = 1

fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0061/RD0061"
REDSHIFT = 2


OUT_FILENAME = f'{COMOVING_BOX_SIZE}Mpc_z{REDSHIFT}_2x2.mp4'
PLOT_TITLE=f"{COMOVING_BOX_SIZE} Mpc box at $z$={REDSHIFT}"
TOTAL_FRAMES = 300

class FieldSetting:
    """
    Class that stores miscellaneous information associated with the fields
    we're querying like the colormap, plot title, units, and colorbar limits. 
    Exists for convenience purposes when plotting and collecting data.
    """

    def __init__(self, units, cmap, title, clim):
        self.units = units
        self.cmap = cmap
        self.title = title
        self.clim = clim

FIELD_SETTINGS = {
    'density' : FieldSetting(
        'g/cm**3',
        'viridis',
        r'Density (g/cm$^3$)',
        (1e-32, 1e-24)
    ),
    'metallicity' : FieldSetting(
        'Zsun',
        'dusk',
        r'Metallicity (Z/Z$_{odot}$)',
        (1e-8, 1)
    ),
    'temperature' : FieldSetting(
        'K',
        'plasma',
        r'Temperature (K)',
        (1e3, 1e7)
    ),
    ('deposit', 'stars_density') : FieldSetting(
        'g/cm**3',
        'Purples_r',
        r'Stellar Density (g/cm$^3$)',
        (1e-33, 1e-24)
    ),
}

FIELDS = list(FIELD_SETTINGS.keys())

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
    L = ds.domain_width[0].to('Mpccm/h')
    dL = (ds.quan(0.2, 'Mpccm/h') / L ).value
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
        frame = TwoByTwoFrame(fsi, fields, img_res)

        # handle the first three fields generically
        for i in range(3):
            field = fields[i]
            units = FIELD_SETTINGS[field].units
            frame[field] = np.array(plot.frb[field].to(units).value)

        # stellar density requires a floor, so we swap it
        # out manually here
        # could also possibly do this in the plot code
        units = FIELD_SETTINGS[('deposit', 'stars_density')].units
        star_density = np.array(plot.frb[('deposit', 'stars_density')].to(units).value)
        star_density[star_density == 0.] = 1e-33

        frame[('deposit', 'stars_density')] = star_density
        
        frame_data.append(frame)

    return frame_data

def Analysis(data):
    print("Stub method for Analysis")

def Plot(data):

    for i, frame in enumerate(data):

        axes = [None]*4
        fig, ((axes[0], axes[1]),(axes[2], axes[3])) = plt.subplots(
            2,2,
            figsize=(WIDTH, HEIGHT))
            # sharex='all',
            # sharey='all')

        fig.suptitle(PLOT_TITLE)
        # plt.subplots_adjust(left=0.1, \
        #                     bottom=0.1,\
        #                     right=0.95,\
        #                     top=0.9,\
        #                     wspace=0.15,\
        #                     hspace=0.01)

        # Fix the x and y axes
        x = np.linspace(0, COMOVING_BOX_SIZE, IMG_RES+1, endpoint=True)
        y = np.linspace(0, COMOVING_BOX_SIZE, IMG_RES+1, endpoint=True)
        X,Y = np.meshgrid(x,y)

        for field,ax in zip(FIELDS, axes):
            im = ax.pcolormesh(X, Y, frame[field], 
            cmap=FIELD_SETTINGS[field].cmap, 
            norm=LogNorm(),
            vmin=FIELD_SETTINGS[field].clim[0],
            vmax=FIELD_SETTINGS[field].clim[1])

            ax.set_aspect('equal')
            

            divider = make_axes_locatable(ax)
            cax = divider.append_axes("right", size="5%", pad=0.05)
            cbar = fig.colorbar(im, cax=cax)
            cbar.set_label(FIELD_SETTINGS[field].title)
        
        axes[2].set_xlabel("Mpc/h")
        axes[2].set_ylabel("Mpc/h")  

        out_file = f'{TEMP_DIR}/tmp_{frame.index:04d}.png'
        print(f"Saving to file: {out_file}")
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(out_file)
        plt.close()

def Finalize():
    print("Running conversion to mp4 format...")
    cmd = f"ffmpeg -y -start_number 0 -framerate 5 -i {TEMP_DIR}/tmp_%04d.png -s 1440x1080 -r 30 -vcodec libx264 -pix_fmt yuv420p {os.path.join(OUT_DIR,OUT_FILENAME)}"
    subprocess.run(cmd, shell=True)

    cmd = f"rm {TEMP_DIR}/tmp_*.png"
    subprocess.run(cmd, shell=True)