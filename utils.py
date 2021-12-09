import pickle, inspect, os, hashlib, datetime, subprocess
from blk import config

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

def exists_in_cache(stage):
    cache_fname = os.path.join(config.CACHE_DIR, stage.result_id)

    cache_hit = os.path.exists(cache_fname)
    # if cache_hit:
    #     print(f"Found cache result for {stage}")
    # else:
    #     print(f"No cache result found for {stage}")
    return cache_hit 

def load_result_from_cache(qhash):
    cache_fname = os.path.join(config.CACHE_DIR, qhash)

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


def save_to_cache(result, qhash):
    cache_fname = os.path.join(config.CACHE_DIR, qhash)
    #print(f"Saving result to file {cache_fname}")
    
    with open(cache_fname, 'wb') as f:
        pickle.dump(result, f, protocol=0)


def rm_from_cache(qhash):
    cache_fname = os.path.join(config.CACHE_DIR, qhash)
    try: 
        os.remove(cache_fname)
    except FileNotFoundError as e:
        pass
    
def format_time(seconds):
    return str(datetime.timedelta(seconds=seconds))

def movie(movie_filename, plot_file_format):
    print("Running conversion to mp4 format...")
    cmd = f"ffmpeg -y -start_number 0 -framerate 10 -i {plot_file_format} -s 1440x1080 -vcodec libx264 -pix_fmt yuv420p {movie_filename}"
    subprocess.run(cmd, shell=True)