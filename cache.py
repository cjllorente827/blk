import pickle, inspect, os, hashlib

CACHE_DIR = "."



def exists(stage):
    global CACHE_DIR
    cache_fname = os.path.join(CACHE_DIR, stage.cache_id)

    cache_hit = os.path.exists(cache_fname)
    # if cache_hit:
    #     print(f"Found cache result for {stage}")
    # else:
    #     print(f"No cache result found for {stage}")
    return cache_hit 

def load(qhash):
    global CACHE_DIR
    cache_fname = os.path.join(CACHE_DIR, qhash)

    # if we have a previous result, serve that up
    try:
        with open(cache_fname, 'rb') as f:
            #print(f"Found cache result for {qhash}")
            result = pickle.load(f)
    except FileNotFoundError as e:
        #print(f"No cache result found for {cache_fname}")
        raise 
    else:
        return result


def save(result, qhash):
    global CACHE_DIR
    cache_fname = os.path.join(CACHE_DIR, qhash)
    #print(f"Saving result to file {cache_fname}")
    
    with open(cache_fname, 'wb') as f:
        pickle.dump(result, f, protocol=0)


def remove(qhash):
    global CACHE_DIR
    cache_fname = os.path.join(CACHE_DIR, qhash)
    try: 
        os.remove(cache_fname)
    except FileNotFoundError as e:
        pass



def set_dir(dirname):
    global CACHE_DIR
    CACHE_DIR = dirname

