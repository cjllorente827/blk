

import gc, yt, time
import datetime
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

import proto_movie

OUT_DIR = "/mnt/home/llorente/comp_structure_research/general_analysis"

def main():
    fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'
    #fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"
    #fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0135/RD0135"
    nframes = 300

    start = time.time()
    proto_movie.Movie("test_proto_movie", fname, nframes, max_procs=16, 
        output_dir=OUT_DIR, out_fname="25Mpc_density_w_metallicity.mp4")

    elapsed = time.time() - start

    print(f"""
Total time: {str(datetime.timedelta(seconds=elapsed))}     
    """)


def Query(ds_fname, frame_start, frame_end, total_frames):

    yt.funcs.mylog.setLevel(50)

    ds = yt.load(ds_fname)
    nframes = frame_end - frame_start

    L = ds.domain_width[0].to('Mpc/h')
    dL = (ds.quan(0.2, 'Mpc/h') / L ).value
    vel = (1.0-dL)/total_frames
    frame_data = np.zeros((nframes, 800, 800))

    gc.disable()
    
    # We need two sets of indices: 
    # one that starts at f_start and ends at f_end -- fsi (frame start index)
    # and the other that starts at zero and ends at f_end-f_start -- zsi (zero start index)
    #
    # the data array must be indexed with the zero start index
    # creation of the slab must use the frame start index
    indexes = zip(range(frame_start, frame_end), range(nframes))

    for fsi,zsi in indexes:
        next_slab = ds.r[:,:,fsi*vel : fsi*vel+dL]
        plot = yt.ProjectionPlot(ds, 'z', 'metallicity', data_source=next_slab, weight_field='density')

        frame = np.array(plot.frb['metallicity'])

        frame_data[zsi][:] = frame[:]

        del frame
        del next_slab
        del plot

        gc.collect()

    gc.enable()
    return frame_data

def Analysis(data):
    print("Stub method for Analysis")

def Plot(data, index):
    fig, ax = plt.subplots(1,1, figsize=(8,6))

    im = ax.imshow(data, origin='lower', 
        norm=LogNorm(), cmap='dusk',
        vmin=1e-8, vmax=1)

    cbar = fig.colorbar(im)
    cbar.set_label(r"Metallicity $(Z/Z_{\odot})$")
    
    def output_fname(n):
        return f'{OUT_DIR}/tmp/tmp_{n:04d}.png'

    out_file = output_fname(index)
    print(f"Saving to file: {out_file}")
    plt.savefig(out_file)
    plt.close()


if __name__ == "__main__":
    main()