import pickle, inspect, os, hashlib, time, shelve

# feels like cheating but whatever, I'm too lazy to parse a 
# whole-ass parameter file
import blk_config

from yt.convenience import load as yt_load
from yt.utilities.exceptions import YTOutputNotIdentified


from mpi4py import MPI


######################################################################
# THIS IS WHERE THE MAGIC HAPPENS
######################################################################
def Do_Query(query_func, *args, clear_cache=False, run_as_root=False):
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
    run_as_root -- Run this function as if it were running on a root process, 
                   meaning that it should output info statements and save
                   files to the cache.
    """

    # get our rank in case we're running in parallel
    rank = MPI.COMM_WORLD.Get_rank()

    root = rank == 0 or run_as_root

    # get the hash
    qhash = get_query_hash(query_func, *args)

    cache_fname = os.path.join(blk_config.CACHE_DIR, qhash)
    # look for files with these extensions as well
    # other_fnames = []
    # for ext in ['.dir', '.h5','.dat', '.bak']:
    #     other_fnames.append(cache_fname+ext)
    

    # sometimes you wanna clear the cache and start over
    if clear_cache and root:
        try: 
            os.remove(cache_fname)
        except FileNotFoundError as e:
            pass

        # for fname in other_fnames:
        #     try:
        #         os.remove(fname)
        #     except FileNotFoundError as e:
        #         pass
    
    # non-root processes don't have to bother returning a result
    # unless specified by the user that they should be run as root
    if root:
        try:
            result, qtime = load_result_from_cache(cache_fname)
        except FileNotFoundError as e:
            pass
        else:
            print(f"Found cache result at {cache_fname}")
            return result, qtime
        

    # if a cache result was not found, perform the actual query
    if root: print(f"No result found for {cache_fname}. Running query....")
    start = time.time()

    result = query_func(*args)

    query_time = time.time() - start

    # and then save the result in the cache
    if root:
        save_to_cache(result, cache_fname)
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

def load_result_from_cache(cache_fname):
    # if we have a previous result, serve that up
    start = time.time()

    # try: 
    #     result = yt_load(cache_fname)
    # except YTOutputNotIdentified as e:
    #     print("yt.load failed, falling back to pickle...")
    # else: 
    #     query_time = time.time() - start
    #     print("Done.")

    #     return result, query_time
    # try: 
    #     d = shelve.open(cache_fname)
    # except FileNotFoundError as e:
    #     print("shelve failed, falling back to pickle...")
    # else: 
    #     query_time = time.time() - start
    #     print("Done.")

    #     result = d["all"]
    #     return result, query_time
    try:
        with open(cache_fname, 'rb') as f:
            result = pickle.load(f)
    except FileNotFoundError as e:
        print(f"No cache result found for {cache_fname}")
        raise 
    else:
        query_time = time.time() - start
        print("Done.")

        return result, query_time


def save_to_cache(result, cache_fname):
    print(f"Saving result to file {cache_fname}")
 
    # try:
    #     result.save_object("all", filename=cache_fname)
    # except AttributeError as e:
    #     print("save_object() failed, falling back to pickle...")
    
    with open(cache_fname, 'wb') as f:
        pickle.dump(result, f, protocol=0)

    print("Done.")
    
    
    

###############################################################
# Constants needed for cohesion
###############################################################

# for distinguishing between the the 3 stages blk operates under
QUERY, ANALYZE, PLOT = 1,2,3