
import yt, subprocess, os, time
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

import blk

from mpi4py import MPI

IMG_RES = 1024

DATA_DIR = "/mnt/gs18/scratch/users/llorente/bigbox/25Mpc_512"
OUTPUT_DIR = "/mnt/home/llorente/comp_structure_research/bigbox_25Mpc/temp"
OUT_FILENAME = "25Mpc_zoom_10Mpc3_x.mp4"
TMP_FILENAME = r"RD%04d_Projection_x_density_density.png"
#TMP_FILENAME = r"RD%04d_OffAxisProjection_temperature_density.png"

def get_dataset_fname(n):
    return f"{DATA_DIR}/RD{n:04d}/RD{n:04d}"

DATASETS = [ get_dataset_fname(n) for n in range(266) ]
#DATASETS = [ get_dataset_fname(n) for n in [28] ]

BOX_ORIGIN = np.array([0.3, 0.26, 0.5])
BOX_LENGTH = 0.4 # 0.4 is 10 Mpccm/h
FIELDS = ["density"]
ROTATIONS = 2

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def main():

    start = time.time()

    N = len(DATASETS)
    start_index = int(rank     /size * N)
    end_index   = int( (rank+1) /size * N)
    for i, ds in enumerate(DATASETS[start_index: end_index]):
        make_projection(ds, start_index+i,
            FIELDS, 
            BOX_ORIGIN, 
            BOX_LENGTH)

    comm.Barrier()
    if rank == 0:
        make_movie()
        elapsed = time.time() - start
        print(f"Execution time: {blk.format_time(elapsed)}")


def make_projection(dataset_fname, index, fields, box_origin, box_length):

    ds = yt.load(dataset_fname)

    x,y,z = box_origin
    dl = box_length
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    plot = yt.ProjectionPlot(ds, 'x', fields,
            data_source=box, 
            center=box_origin + 0.5 * box_length,
            width=box_length,
            weight_field='density', 
            buff_size=(IMG_RES, IMG_RES))
    plot.set_cmap('all', 'viridis')
    plot.set_axes_unit('Mpccm/h')
    #plot.set_zlim("temperature", 1e-3, 1e-8)
    plot.annotate_timestamp(corner="upper_left", redshift=True, draw_inset_box=True)
    plot.annotate_scale(corner="upper_right")
    plot.save(OUTPUT_DIR+'/')

def make_rotating_projection(dataset_fname, index, fields, box_origin, box_length):

    ds = yt.load(dataset_fname)

    x,y,z = box_origin
    dl = box_length
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    tht = 2 * np.pi * ROTATIONS * index / len(DATASETS)
    R0 = np.array([0,0,1])
    Ry = np.array([
        [np.cos(tht), 0, np.sin(tht)],
        [0, 1, 0],
        [-np.sin(tht), 0, np.cos(tht)]
    ])
    rot_axis = Ry @ R0

    plot = yt.OffAxisProjectionPlot(ds, rot_axis, fields,
            data_source=box, 
            north_vector=(0,1,0),
            center=box_origin + 0.5 * box_length,
            width=box_length,
            weight_field='density', 
            buff_size=(IMG_RES, IMG_RES))
    plot.set_cmap('all', 'jet')
    plot.set_axes_unit('Mpccm/h')
    plot.set_zlim("temperature", 1e-3, 1e-8)
    plot.annotate_timestamp(corner="upper_left", redshift=True, draw_inset_box=True)
    plot.annotate_scale(corner="upper_right")
    plot.save(OUTPUT_DIR+'/')

def make_movie():
    print("Running conversion to mp4 format...")
    cmd = f"ffmpeg -y -start_number 0 -framerate 5 -i {OUTPUT_DIR}/{TMP_FILENAME} -s 1440x1080 -r 30 -vcodec libx264 -pix_fmt yuv420p {os.path.join(OUTPUT_DIR,OUT_FILENAME)}"
    subprocess.run(cmd, shell=True)

    cmd = f"rm {OUTPUT_DIR}/*.png"
    subprocess.run(cmd, shell=True)


if __name__ == "__main__":
    main()
