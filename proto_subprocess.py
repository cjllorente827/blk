from mpi4py import MPI
import importlib
import numpy as np

import blk

parent_comm = MPI.Comm.Get_parent()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

params = None
params = parent_comm.bcast(params, root=0)

user_defined = importlib.import_module(params["module_name"])

def Query_subprocess(args):

    query_hash, qtime = blk.Execute_And_Cache_Result(user_defined.Query,
        *args, 
        run_as_root=True, 
        return_hash=True,
        clear_cache=params["clear_cache"])

    parent_comm.gather(query_hash, root=0)

    parent_comm.Disconnect()

def Analyze_subprocess(args):

    query_hash = None
    query_hash = parent_comm.scatter(query_hash, root=0)

    data, load_time = blk.load_result_from_cache(query_hash)

    # prepend the data from the last result to the arguments passed
    # to the execution function
    args.insert(0, data)

    thash, time = blk.Execute_And_Cache_Result(user_defined.Analyze,
        *args, 
        run_as_root=True, 
        return_hash=True,
        clear_cache=params["clear_cache"])

    parent_comm.gather(thash, root=0)
    
    parent_comm.Disconnect()

def Plot_subprocess(args):
    
    track_hash = None
    track_hash = parent_comm.scatter(track_hash, root=0)

    track, load_time = blk.load_result_from_cache(track_hash)

    user_defined.Plot(track, *args)

    finished = True
    parent_comm.gather(finished, root=0)
    
    parent_comm.Disconnect()


if __name__ == "__main__":
    
    if params["stage"] == blk.QUERY:
        Query_subprocess(params["args"])
    elif params["stage"] == blk.ANALYZE:
        Analyze_subprocess(params["args"])
    elif params["stage"] == blk.PLOT:
        Plot_subprocess(params["args"])