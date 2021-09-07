from mpi4py import MPI
import sys, time, importlib, datetime, argparse

import blk

def Pipeline(user_defined, 
    num_procs=16,
    force_query=False,
    force_analyze=False,
    do_plot=True,
    do_final=True):
    """
    Manages the entire analysis pipeline. By default, this function attempts to 
    create a plot by doing the minimal possible work. I.e. if previous results from
    analysis exist, it will only run the Plot function without redoing either the Query
    or Analyze functions. The user may also force any or all functions to run by setting
    the force/do_XXXX arguments for the different stages.

    user_defined -- The user-written module that contains a Query, Analyze, Plot, and 
                    Finalize function, to be executed within this pipeline. This
                    file should also contain arguments to be passed to each function

    Keyword arguments:

    num_procs       --  Number of worker processes to start
    force_query     --  Force a rerun of the Query function
                        If False, will only execute Query if needed
    force_analyze   --  Force a rerun of the Analyze function if True
                        If False, will only execute Analyze if needed
    do_plot         --  Force a rerun of the Plot function
                        Ignore Plot function if False
    do_final        --  Force a rerun of the Finalize function if True
                        Ignore Finalize function if False
    """

    # the name of the subprocess file that runs in the worker processes
    # this file contains a manager process that starts the others and then
    # collects the results of their work
    subprocess = "proto_subprocess.py"

    user_module = importlib.import_module(user_defined)

    # Special Note: Calling get_query_hash this way does NOT append the 
    # process rank to the hash and therefore will result in a different
    # hash than the rank 0 subprocess returns. This is important as 
    # the null rank hash becomes the name of a file that stores the hashes
    # from all the other processes.
    query_hash_filename = blk.get_func_hash(user_module.Query, user_module.QUERY_ARGS)

    # get the track hashes, stored the same way as the query hashes, but these      
    # represent the plot-ready "track" data created by the Analysis stage
    track_hash_filename = blk.get_func_hash(user_module.Analyze, user_module.ANALYZE_ARGS)  

    # if we're force re-doing the query or analysis then clear out any
    # previous results
    if force_query:
        blk.rm_from_cache(query_hash_filename)
    if force_analyze:
        blk.rm_from_cache(track_hash_filename)

    # attempt to load previous results from the query and analysis stages
    # the results will help us determine how much work needs to be done

    try:
        query_hashes, query_time = blk.load_result_from_cache(query_hash_filename)
    except FileNotFoundError as e:
        # we didn't find query results
        query_hashes = None
    else:
        print(f"Found query result for \"{user_defined}\" at {query_hash_filename}")

    try:
        track_hashes, track_time = blk.load_result_from_cache(track_hash_filename)
    except FileNotFoundError as e:
        # we didn't find a track
        track_hashes = None
    else:
        print(f"Found track for \"{user_defined}\" at {track_hash_filename}")


    # Figure out how much work we're doing
    # Note how this allows for the analysis and query stages to be run
    # without creating a plot, if desired
    do_analyze = (do_plot and track_hashes == None) or force_analyze
    do_query = (do_analyze and query_hashes == None) or force_query


    # Define the execution functions for each stage of the pipeline
    def execute_Query(query_hash_filename):
        params = {
            "module_name" : user_defined,
            "max_procs"   : num_procs,
            "clear_cache" : force_query,
            "args"        : user_module.QUERY_ARGS,
            "stage"       : blk.QUERY
        }

        #spawn the query processes
        print("Executing query...")

        start = time.time()
        comm, size = spawn_comm(subprocess, num_procs, params)

        query_hashes = []
        query_hashes = comm.gather(query_hashes, root=MPI.ROOT)

        comm.Disconnect()
        
        blk.save_to_cache(query_hashes, query_hash_filename)

        elapsed = time.time() - start
        print(f"Query completed after {format_time(elapsed)}")
        return query_hashes

    def execute_Analyze(query_hashes, track_hash_filename):
        params = {
            "module_name" : user_defined,
            "max_procs"   : num_procs,
            "clear_cache" : force_analyze, 
            "args"        : user_module.ANALYZE_ARGS,
            "stage"       : blk.ANALYZE
        }

        #spawn the query processes
        print("Starting analysis...")

        start = time.time()
        comm, size = spawn_comm(subprocess, num_procs, params)

        
        comm.scatter(query_hashes, root=MPI.ROOT)

        track_hashes = []
        track_hashes = comm.gather(track_hashes, root=MPI.ROOT)

        comm.Disconnect()
        
        blk.save_to_cache(track_hashes, track_hash_filename)

        elapsed = time.time() - start
        print(f"Analysis completed after {format_time(elapsed)}")
        return track_hashes

    def execute_Plot(track_hashes):

        params = {
            "module_name" : user_defined,
            "max_procs"   : num_procs,
            "clear_cache" : False, 
            "args"        : user_module.PLOT_ARGS,
            "stage"       : blk.PLOT
        }

        print("Creating plot...")
        start = time.time()

        # spawn the plot processes
        comm, size = spawn_comm(subprocess, num_procs, params)

        comm.scatter(track_hashes, root=MPI.ROOT)

        all_finished = None
        all_finished = comm.gather(all_finished, root=MPI.ROOT)

        assert len(all_finished) == num_procs
        for done in all_finished:
            assert done

        comm.Disconnect()

        elapsed = time.time() - start
        print(f"Plot done after {elapsed}")

    # Finally, execute the pipeline
    if do_query:
        query_hashes = execute_Query(query_hash_filename)

    if do_analyze:
        track_hashes = execute_Analyze(query_hashes, track_hash_filename)

    if do_plot:
        execute_Plot(track_hashes)

    if do_final:
        print("Finalizing...")
        user_module.Finalize()

    print("All done")


def spawn_comm(subprocess, max_procs, params):
    comm = MPI.COMM_SELF.Spawn(sys.executable, 
        args=[subprocess], 
        maxprocs=max_procs)

    comm.Set_errhandler(MPI.ERRORS_ARE_FATAL)
    size = comm.Get_size()

    
    comm.bcast(params, root=MPI.ROOT)

    return comm, size

def format_time(seconds):
    return str(datetime.timedelta(seconds=seconds))

if __name__ == "__main__":

    desc = """
Runs an analysis pipeline consisting of three separate stages and one optional 
finalization step. Each result from the previous stage is saved to a cache directory
allowing for that stage to be skipped on subsequent runs. This allows for active 
development of plots and analysis methods without having to wait on long-running
dataset hierarchy parsing operations. 
"""

    usage = """
python blk_pipeline.py [-n <integer>] [-qapf] <user defined python module>    

-n -- number of worker processes to spawn
-q -- force query execution 
-a -- force analysis execution 
-p -- force plot creation 
-f -- force finalization code 

any combination of the 4 above will run the minimum amount of work required 
to complete those stages. 
specifying none of these stages will result in the code attempting to 
do as little work as possible to create a plot, i.e. same as setting -pf
"""
    parser = argparse.ArgumentParser(usage=usage, description=desc)
    parser.add_argument('-n', type=int, default=16)
    parser.add_argument('-q', action="store_true", default=False)
    parser.add_argument('-a', action="store_true", default=False)
    parser.add_argument('-p', action="store_true", default=False)
    parser.add_argument('-f', action="store_true", default=False)
    parser.add_argument('user_defined')
    args = vars(parser.parse_args())

    use_default_stages = not(args["q"] or args["a"] or args["p"] or args["f"])

    start = time.time()
    if use_default_stages:
        Pipeline(args["user_defined"], 
        num_procs=args["n"])
    else:
        Pipeline(args["user_defined"], 
            num_procs=args["n"],
            force_query=args["q"],
            force_analyze=args["a"],
            do_plot=args["p"],
            do_final=args["f"])

    elapsed = time.time() - start

    print(f"""
Total time: {format_time(elapsed)}     
    """)


