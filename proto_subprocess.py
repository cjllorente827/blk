from mpi4py import MPI
import importlib
import numpy as np

import blk

comm = MPI.Comm.Get_parent()
size = comm.Get_size()
rank = comm.Get_rank()

params = None
params = comm.bcast(params, root=0)

user_defined = importlib.import_module(params["module_name"])

nframes = params["nframes"]
ds_fname = params["ds_fname"]

f_start = int(rank/size * nframes)
f_end = int ( (rank+1)/size * nframes )

def Query_subprocess():

    data, qtime = blk.Do_Query(user_defined.Query,
        ds_fname, f_start, f_end, nframes, run_as_root=True)

    comm.gather(data, root=0)

    comm.Disconnect()

def Plot_subprocess():
    
    data = None
    data = comm.scatter(data, root=0)

    # We need two sets of indices: 
    # one that starts at f_start and ends at f_end -- fsi (frame start index)
    # and the other that starts at zero and ends at f_end-f_start -- zsi (zero start index)
    #
    # the data array must be indexed with the zero_start index
    # the plot function takes the frame start index as an argument for correct file output naming
    indexes = zip(range(f_start, f_end), range(f_end - f_start))

    for fsi,zsi in indexes:
        user_defined.Plot(data[zsi], fsi)

    finished = True
    comm.gather(finished, root=0)
    
    comm.Disconnect()


if __name__ == "__main__":
    
    if params["stage"] == blk.QUERY:
        Query_subprocess()
    elif params["stage"] == blk.PLOT:
        Plot_subprocess()