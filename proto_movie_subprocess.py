from mpi4py import MPI
import importlib
import numpy as np

import blk

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()

def main():
    print(f"Hello from rank {rank}!")

    params = None
    params = comm.bcast(params, root=0)

    print(params["module_name"])
    user_defined = importlib.import_module(params["module_name"])

    nframes = params["nframes"]
    ds_fname = params["ds_fname"]
    

    f_start = int(rank/size * nframes)
    f_end = int ( (rank+1)/size * nframes )

    data = blk.Do_Query(user_defined.Query,
        ds_fname, f_start, f_end, run_as_root=True)

    complete = True
    comm.gather(complete, root=0)
    


main()