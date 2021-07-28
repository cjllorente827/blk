from mpi4py import MPI
import numpy as np
import sys, subprocess, time, os, importlib, datetime

import blk

def Do_Pipeline(user_defined, 
    num_procs=16,
    clear_cache=False, 
    output_dir='.', 
    temp_dir='temp'):

    subprocess = "proto_subprocess.py"

    user_module = importlib.import_module(user_defined)

    qhash_filename = blk.get_query_hash(user_module.Query, user_module.QUERY_ARGS)

    if clear_cache:
        blk.rm_from_cache(qhash_filename)
    try:
        # attempt to load the file containing the hashes of the last results
        qhashes, query_time = blk.load_result_from_cache(qhash_filename)
    except FileNotFoundError as e:
        found_query_in_cache = False
    else:
        print(f"Found cached result for \"{user_defined}\" at {qhash_filename}")
        found_query_in_cache = True

    params = {
        "module_name" : user_defined,
        "max_procs"   : num_procs,
        "clear_cache" : clear_cache, 
        "output_dir"  : output_dir, 
        "temp_dir"    : temp_dir,
        "args"        : user_module.QUERY_ARGS,
        "stage"       : blk.QUERY
    }

    if not found_query_in_cache:
        #spawn the query processes
        comm, size = spawn_comm(subprocess, num_procs, params)

        print("Gathering data...")
        qhashes = []
        qhashes = comm.gather(qhashes, root=MPI.ROOT)

        comm.Disconnect()
        print("Query subprocessing complete")
        
        blk.save_to_cache(qhashes, qhash_filename)


    print("Spawning plot subprocesses")

    # change the stage to reflect the next part of the process
    params["args"]  = user_module.PLOT_ARGS
    params["stage"] = blk.PLOT

    # spawn the plot processes
    comm, size = spawn_comm(subprocess, num_procs, params)

    print("Scattering data...")
    print(qhashes)
    comm.scatter(qhashes, root=MPI.ROOT)

    print("Gathering results...")
    all_finished = None
    all_finished = comm.gather(all_finished, root=MPI.ROOT)

    assert len(all_finished) == num_procs
    for done in all_finished:
        assert done

    comm.Disconnect()

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

if __name__ == "__main__":
    user_defined = sys.argv[1]

    start = time.time()
    Do_Pipeline(user_defined)
    elapsed = time.time() - start

    print(f"""
Total time: {str(datetime.timedelta(seconds=elapsed))}     
    """)

    
