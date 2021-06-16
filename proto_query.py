#########################################################
# Prototype script to test out the limitations of query 
# hashing and recall
#########################################################

import pickle, inspect, os, hashlib
from time import time, strftime, gmtime

CACHE_DIR = "/mnt/gs18/scratch/users/llorente/blk_qcache"

def Do_Query(query_func, *args, clear_cache=False):

    # get the hash
    qhash = get_query_hash(query_func, *args)

    
    cache_fname = os.path.join(CACHE_DIR, qhash)
    cache_entry_exists = os.path.isfile(cache_fname)
    
    # sometimes you wanna clear the cache and start over
    if clear_cache and cache_entry_exists:
        os.remove(cache_fname)
        cache_entry_exists = False

    # check the qcache  
    if cache_entry_exists:
            
        # if we have a previous result, serve that up
        print(f"Found cache result at: {cache_fname}")
        start = time()
        with open(cache_fname, 'rb') as f:
            result = pickle.load(f)
        end = time() - start
        query_time = format_time(end)
        print("Done.")
        return result, query_time
    
    # otherwise run the actual query
    print(f"No result found for {cache_fname}. Running query....")
    start = time()
    result = query_func(*args)
    end = time() - start
    query_time = format_time(end)

    # and then save the result in the cache
    print(f"Saving result to file {cache_fname}")
    with open(cache_fname, 'wb') as f:
        pickle.dump(result, f)

    # and finally return 
    print("Done.")
    return result, query_time

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


def format_time(num_seconds):
    return strftime('%H:%M:%S', gmtime(num_seconds) )