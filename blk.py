import pickle, inspect, os, hashlib, time

# feels like cheating but whatever, I'm too lazy to parse a 
# whole-ass parameter file
import blk_config

from mpi4py import MPI


######################################################################
# THIS IS WHERE THE MAGIC HAPPENS
######################################################################
def Execute_And_Cache_Result(func, *args, 
    clear_cache=False, 
    run_as_root=False, 
    return_hash=False):
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

    func -- the function that queries the dataset. This must return
                  the result of the query as some kind of serializable object.
                  It can take any arbitrary positional arguments. 

    Keyword arguments:
    clear_cache --  Clear the cache result found for this query and rerun it 
                    (Default: False)
    run_as_root -- Run this function as if it were running on a root process, 
                   meaning that it should output info statements and save
                   files to the cache.
    return_hash -- Instead of attempting to return data, instead return the
                    hash that corresponds to the file where the data is stored.
                    Still runs the query if the file can not be found.
    """

    # get our rank in case we're running in parallel
    rank = MPI.COMM_WORLD.Get_rank()
    root = rank == 0 or run_as_root

    # get the hash
    qhash = get_func_hash(func, *args, rank=rank)


    # sometimes you wanna clear the cache and start over
    if clear_cache and root:
        rm_from_cache(qhash)
    
    # non-root processes don't have to bother returning a result
    # unless specified by the user that they should be run as root
    if root:
        try:
            result, query_time = load_result_from_cache(qhash)
        except FileNotFoundError as e:
            pass
        else:
            print(f"Found cache result at {qhash}")
            if return_hash:
                return qhash, query_time
            return result, query_time
        

    # if a cache result was not found, perform the actual query
    if root: print(f"No result found for {qhash}. Running query....")
    start = time.time()

    result = func(*args)

    query_time = time.time() - start

    # and then save the result in the cache
    if root:
        save_to_cache(result, qhash)
        if return_hash:
            return qhash, query_time
        return result, query_time
    else: 
        return None, None

def get_func_hash(func, *args, rank=None):

    # get the source code of the Query function
    source = inspect.getsource(func)

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

    # add the rank if not None, i.e. the pipeline dispatcher
    # process needs to save something
    # running Do_Query by itself adds rank=0 by default
    if rank is not None:
        target += str(rank)

    # convert string to a hash
    qhash = hashlib.md5(target.encode()).hexdigest()
    return qhash

def load_result_from_cache(qhash):
    cache_fname = os.path.join(blk_config.CACHE_DIR, qhash)

    # if we have a previous result, serve that up
    start = time.time()

    try:
        with open(cache_fname, 'rb') as f:
            result = pickle.load(f)
    except FileNotFoundError as e:
        print(f"No cache result found for {cache_fname}")
        raise 
    else:
        load_time = time.time() - start

        return result, load_time


def save_to_cache(result, qhash):
    cache_fname = os.path.join(blk_config.CACHE_DIR, qhash)
    print(f"Saving result to file {cache_fname}")
    
    with open(cache_fname, 'wb') as f:
        pickle.dump(result, f, protocol=0)


def rm_from_cache(qhash):
    cache_fname = os.path.join(blk_config.CACHE_DIR, qhash)
    try: 
        os.remove(cache_fname)
    except FileNotFoundError as e:
        pass
    
    

###############################################################
# Constants needed for cohesion
###############################################################

# for distinguishing between the the 3 stages blk operates under
QUERY, ANALYZE, PLOT = 1,2,3