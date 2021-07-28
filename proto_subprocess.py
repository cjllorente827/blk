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

def Query_subprocess(args):

    qhash, qtime = blk.Do_Query(user_defined.Query,
        *args, 
        run_as_root=True, 
        return_hash=True,
        clear_cache=params["clear_cache"])

    comm.gather(qhash, root=0)

    comm.Disconnect()

def Plot_subprocess():
    
    datahash = None
    datahash = comm.scatter(datahash, root=0)

    data, load_time = blk.load_result_from_cache(datahash)

    user_defined.Plot(data)

    finished = True
    comm.gather(finished, root=0)
    
    comm.Disconnect()


if __name__ == "__main__":
    
    if params["stage"] == blk.QUERY:
        Query_subprocess(params["args"])
    elif params["stage"] == blk.PLOT:
        Plot_subprocess()