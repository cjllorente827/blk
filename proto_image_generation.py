from mpi4py import MPI
import importlib
import numpy as np

import blk

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()

def ImageGenerate():

    params = None
    params = comm.bcast(params, root=0)

    user_defined = importlib.import_module(params["module_name"])

    nframes = params["nframes"]
    ds_fname = params["ds_fname"]
    

    f_start = int(rank/size * nframes)
    f_end = int ( (rank+1)/size * nframes )


    comm.gather(data, root=0)

    comm.Disconnect()
    

if __name__ == "__main__":
    ImageGenerate()