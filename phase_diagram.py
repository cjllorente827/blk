import mpi4py
import yt, subprocess, os, sys
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from mpi4py import MPI

yt.funcs.mylog.setLevel(50)

plt.style.use("publication")

OUT_DIR = "/mnt/home/llorente/comp_structure_research/research_notes/imgs"
TEMP_DIR = f"{OUT_DIR}/temp"

# fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'

# fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
# PLOT_TITLE = r"25 Mpc box at $z = 0$"
# PLOT_FNAME = "25Mpc_phase_z0"

# fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0166/RD0166"
# PLOT_TITLE = r"25 Mpc box at $z = 1$"
# PLOT_FNAME = "25Mpc_phase_z1"

fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0111/RD0111"
PLOT_TITLE = r"25 Mpc box at $z = 2$"
PLOT_FNAME = "25Mpc_phase_z2"

QUERY_ARGS = [fname]
ANALYZE_ARGS = [fname]
PLOT_ARGS = []

def Query(dataset):

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    yt.funcs.mylog.setLevel(50)

    ds = yt.load(dataset)

    ad = ds.r[rank/size:(rank+1)/size,:,:]

    logT = np.log10(ad["temperature"].to('K'))
    logrho = np.log10(ad["density"].to('g/cm**3'))
    mass = np.array(ad[('gas', 'mass')].to('Msun'))

    # quick check to ensure all our data has the same size
    assert len(logrho) == len(logT) and len(logrho) == len(mass)

    data = [logT, logrho, mass]

    return data

def Analyze(data, dataset_name):

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    # figure out the minimum and maximum of the data for each 
    # dimension so we can ensure the bins are the same for 
    # each process

    my_min_T = np.min(data[0])
    my_max_T = np.max(data[0])

    my_min_rho = np.min(data[1])
    my_max_rho = np.max(data[1])

    min_T = 1e80
    max_T = -1
    min_rho = 1e80
    max_rho = -1

    min_T = comm.allreduce(my_min_T, op=MPI.MIN)
    max_T = comm.allreduce(my_max_T, op=MPI.MAX)

    min_rho = comm.allreduce(my_min_rho, op=MPI.MIN)
    max_rho = comm.allreduce(my_max_rho, op=MPI.MAX)

    hist, x, y = np.histogram2d(data[1], data[0], 
                              bins=128, 
                              range = [[min_rho, max_rho],[min_T, max_T]],
                              weights=data[2])

    my_masses = np.array(hist.T)
    masses = np.zeros_like(my_masses)

    comm.Reduce(my_masses, masses, op=MPI.SUM, root=0)

    data = [x,y,masses]

    return data

def Plot(track):

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank != 0:
        return

    plt.pcolormesh( 10**track[0], 10**track[1], track[2],
            norm=mcolors.LogNorm(), cmap="plasma",
            vmin=1.,
            vmax=1e13)
    ax = plt.gca()

    ax.set(
        xlabel=r'Density $\left( \frac{\rm{g}}{\rm{cm}^3} \right)$',
        ylabel=r"Temperature ($\rm{K}$)",
        xscale='log',
        yscale='log')
    cbar = plt.colorbar(pad=0)
    cbar.set_label(r"Total Mass ($\rm{M_{\odot}}$)")
    cbar.ax.tick_params(direction = 'out')


    plt.title(PLOT_TITLE)

    plot_fname = os.path.join(OUT_DIR, PLOT_FNAME) 
    print(f"Saving plot to {plot_fname}.png")
    plt.savefig(plot_fname)
    plt.close()

def Finalize():
    pass
    # print("Running conversion to mp4 format...")
    # cmd = f"ffmpeg -y -start_number 0 -framerate 5 -i {TEMP_DIR}/tmp_%04d.png -s 1440x1080 -r 30 -vcodec libx264 -pix_fmt yuv420p {os.path.join(OUT_DIR, OUT_FILENAME)}"
    # subprocess.run(cmd, shell=True)

    # cmd = f"rm {TEMP_DIR}/tmp_*.png"
    # subprocess.run(cmd, shell=True)