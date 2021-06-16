import pickle, inspect, os, hashlib, time

# feels like cheating but whatever, I'm too lazy to parse a 
# whole-ass parameter file
import blk_config

from mpi4py import MPI


######################################################################
# THIS IS WHERE THE MAGIC HAPPENS
######################################################################
def Do_Query(query_func, *args, clear_cache=False):
    """
    Attempts to perform a dataset query function with *args passed to that 
    function, but will first check blk_config.CACHE_DIR to see if that query
    has been run and if a result for it already exists. If so, it returns
    that result and the time it took to load that result in seconds. If not, 
    it runs the function with the supplied arguments and saves the result in
    blk_config.CACHE_DIR, then returns that result along with the time it 
    took to run the query in seconds. The user may also supply the keyword 
    argument clear_cache=True to force delete any result found for that 
    query in the cache, then re-run the query function as normal. 

    query_func -- the function that queries the dataset. This must return
                  the result of the query as some kind of serializable object.
                  It can take any arbitrary positional arguments. 

    Keyword arguments:
    clear_cache --  Clear the cache result found for this query and rerun it 
                    (Default: False)
    """

    # get our rank in case we're running in parallel
    rank = MPI.COMM_WORLD.Get_rank()

    # get the hash
    qhash = get_query_hash(query_func, *args)

    
    cache_fname = os.path.join(blk_config.CACHE_DIR, qhash)
    cache_entry_exists = os.path.isfile(cache_fname)
    
    # sometimes you wanna clear the cache and start over
    if clear_cache and cache_entry_exists:
        if rank == 0 : os.remove(cache_fname)
        cache_entry_exists = False
        

    # check the qcache  
    if cache_entry_exists:
        
        if rank == 0:
            # if we have a previous result, serve that up
            print(f"Found cache result at: {cache_fname}")
            start = time.time()

            
            with open(cache_fname, 'rb') as f:
                result = pickle.load(f)

            query_time = time.time() - start
            print("Done.")
            return result, query_time
        else: 
            return None, None
    
    # otherwise run the actual query
    if rank == 0: print(f"No result found for {cache_fname}. Running query....")
    start = time.time()

    result = query_func(*args)

    query_time = time.time() - start

    # and then save the result in the cache
    if rank == 0:
        print(f"Saving result to file {cache_fname}")
        with open(cache_fname, 'wb') as f:
            pickle.dump(result, f)

        # and finally return 
        print("Done.")
        return result, query_time
    else: 
        return None, None

def get_query_hash(query_func, *args):

    # get the source code of the Query function
    source = inspect.getsource(query_func)

    def remove_all_whitespace(str):
        ws_chars = [' ', '\t', '\n']
        for char in ws_chars:
            str = str.replace(char, '')
        return str

    def append_args(target, args):
        for a in args:
            target += str(a)
        return target

    # scrap the whitespace to prevent unnecessary 
    # re-queries
    source_no_ws = remove_all_whitespace(source)

    # concatenate it with the arguments
    target = append_args(source_no_ws, args)

    # convert string to a hash
    qhash = hashlib.md5(target.encode()).hexdigest()
    return qhash