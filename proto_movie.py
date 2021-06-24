from mpi4py import MPI
import numpy as np
import sys
import time



def Movie(module_name, ds_fname, nframes, max_procs=16, fpp=10):

    comm = MPI.COMM_SELF.Spawn(sys.executable, 
        args=['proto_movie_subprocess.py'], 
        maxprocs=max_procs)

    comm.Set_errhandler(MPI.ERRORS_ARE_FATAL)
    size = comm.Get_size()

    params = {
        "module_name" : module_name,
        "ds_fname"    : ds_fname,
        "nframes"     : nframes, 
        "max_procs"   : max_procs,
        "fpp"         : fpp
    }

    comm.bcast(params, root=MPI.ROOT)

    complete = False
    all_complete = comm.gather(complete, root=MPI.ROOT)

    for i in range(max_procs):
        assert all_complete[i]

    print("All done")

    

    

    
