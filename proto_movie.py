from mpi4py import MPI
import numpy as np
import sys, subprocess, time, os

import blk

def Movie(module_name, ds_fname, nframes, max_procs=16, fpp=100, output_dir='.', tmp_dir='tmp', out_fname='movie.mp4'):

    subprocess = "proto_subprocess.py"

    params = {
        "module_name" : module_name,
        "ds_fname"    : ds_fname,
        "nframes"     : nframes, 
        "max_procs"   : max_procs,
        "fpp"         : fpp,
        "stage"       : blk.QUERY
    }

    comm, size = spawn_comm(subprocess, max_procs, params)

    print("Gathering data...")
    sendbuf = None
    recvbuf = np.zeros((size, nframes, 4, 1024, 1024))
    data = comm.Gather(sendbuf, recvbuf, root=MPI.ROOT)

    comm.Disconnect()
    print("Query subprocessing complete")

    print("Spawning plot subprocesses")

    # change the stage to reflect the next part of the process
    params["stage"] = blk.PLOT

    comm, size = spawn_comm(subprocess, max_procs, params)

    print("Scattering data...")
    comm.scatter(data, root=MPI.ROOT)

    print("Gathering results...")
    all_finished = None
    all_finished = comm.gather(all_finished, root=MPI.ROOT)

    assert len(all_finished) == max_procs
    for done in all_finished:
        assert done

    comm.Disconnect()

    runConversion(20, output_dir, tmp_dir, out_fname)

    cleanup(output_dir, tmp_dir)

    print("All done")


def spawn_comm(subprocess, max_procs, params):
    comm = MPI.COMM_SELF.Spawn(sys.executable, 
        args=[subprocess], 
        maxprocs=max_procs)

    comm.Set_errhandler(MPI.ERRORS_ARE_FATAL)
    size = comm.Get_size()

    print("Broadcasting parameters...")
    comm.bcast(params, root=MPI.ROOT)
    print("Done")

    return comm, size


def runConversion(interval, output_dir, tmp_dir, out_fname):
    
    frames = os.path.join(output_dir, tmp_dir, "tmp_%04d.png")
    out_file = os.path.join(output_dir, out_fname)
    #cmd = f"convert -delay {interval} -loop 0 {frames} {out_file}"
    cmd = f"ffmpeg -start_number 0 -framerate 5 -i {frames} -s 1080x720 -r 30 -vcodec libx264 -pix_fmt yuv420p {out_fname}"
    print(f"""
Running conversion: 
    {cmd}
""")
    
    subprocess.run(cmd, shell=True)

    print("Done")

def cleanup(output_dir, tmp_dir):
    print("Removing files from tmp")
    frames = os.path.join(output_dir, tmp_dir, "*.png")
    print(f"rm {frames}")
    #subprocess.run(['ls', 'tmp'])
    #subprocess.run(f'rm {frames}', shell=True)
    

    
