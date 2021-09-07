from mpi4py import MPI
import sys, time, importlib, datetime

import blk

def Do_Pipeline(user_defined, 
    num_procs=16,
    clear_cache=False, 
    output_dir='.', 
    temp_dir='temp'):

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
    query_hash_filename = blk.get_query_hash(user_module.Query, user_module.QUERY_ARGS)

    # get the track hashes, stored the same way as the query hashes, but these      
    # represent the plot-ready "track" data created by the Analysis stage
    track_hash_filename = blk.get_query_hash(user_module.Analyze, user_module.ANALYZE_ARGS)  

    def execute_Query(query_hash_filename):
        params = {
            "module_name" : user_defined,
            "max_procs"   : num_procs,
            "clear_cache" : clear_cache, 
            "args"        : user_module.QUERY_ARGS,
            "stage"       : blk.QUERY
        }

        #spawn the query processes
        print("Executing query...")

        start = time.time()
        comm, size = spawn_comm(subprocess, num_procs, params)

        print("Gathering data...")
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
            "clear_cache" : clear_cache, 
            "args"        : user_module.ANALYZE_ARGS,
            "stage"       : blk.ANALYZE
        }

        #spawn the query processes
        print("Executing analysis...")

        start = time.time()
        comm, size = spawn_comm(subprocess, num_procs, params)

        print("Scattering data...")
        comm.scatter(query_hashes, root=MPI.ROOT)

        print("Gathering data...")
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
            "clear_cache" : clear_cache, 
            "args"        : user_module.PLOT_ARGS,
            "stage"       : blk.PLOT
        }

        print("Executing plot...")

        # spawn the plot processes
        comm, size = spawn_comm(subprocess, num_procs, params)

        print("Scattering data...")
        comm.scatter(track_hashes, root=MPI.ROOT)

        print("Gathering results...")
        all_finished = None
        all_finished = comm.gather(all_finished, root=MPI.ROOT)

        assert len(all_finished) == num_procs
        for done in all_finished:
            assert done

        comm.Disconnect()

    # clear all relevant files from the cache if that's what the user wants
    if clear_cache:
        blk.rm_from_cache(query_hash_filename)
        blk.rm_from_cache(track_hash_filename)

    # attempt to load the file containing the hashes of the track files
    try:
        track_hashes, query_time = blk.load_result_from_cache(track_hash_filename)
    except FileNotFoundError as e:
        # we didn't find track files, so try to find query results for the data
        # required to create the tracks
        found_track_in_cache = False
    else:
        found_track_in_cache = True
        print(f"Found cached result for \"{user_defined}\" at {track_hash_filename}")

    if not found_track_in_cache:
        try:
            query_hashes, query_time = blk.load_result_from_cache(query_hash_filename)
        except FileNotFoundError as e:
            # we didn't find query files, so run the query and make them
            query_hashes = execute_Query(query_hash_filename)
        else:
            print(f"Found cached result for \"{user_defined}\" at {query_hash_filename}")

        # run analysis once we've got our query data        
        track_hashes = execute_Analyze(query_hashes, track_hash_filename)

    # make the actual plot
    execute_Plot(track_hashes)

    # run user-defined code for making movies/cleaning up temporary files
    print("Finalizing...")

    user_module.Finalize()

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

def format_time(seconds):
    return str(datetime.timedelta(seconds=seconds))

if __name__ == "__main__":
    user_defined = sys.argv[1]

    start = time.time()
    Do_Pipeline(user_defined)
    elapsed = time.time() - start

    print(f"""
Total time: {format_time(elapsed)}     
    """)


