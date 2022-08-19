
from os import listdir, remove
from os.path import isfile, join

import pickle

from constants import AUTO, MANUAL

class Cache:

    def __init__(self, directory):

        self.directory = directory

        # keeps track of what exists in the real cache
        self.virtual_cache = set()

        self.update()

    def __str__(self):
        my_str = "Cache contains:\n"
        for c in self.virtual_cache:
            my_str += f"{c}\n"
        return my_str


    # reads the real cache (the disk) and updates the virtual cache based on 
    # what it finds
    def update(self):
        
        ls = listdir(self.directory)
        for fname in ls:
            cache_fname = join(self.directory, fname)
            if isfile(cache_fname):
                self.virtual_cache.add(fname)
                

    def hasResultFor(self, task):

        if task.save_action == AUTO:
            return task.hashcode in self.virtual_cache
        elif task.save_action == MANUAL:
            return task.output_file in self.virtual_cache
        else :
            return False

    def load(self, task):

        cache_fname = join(self.directory, task.hashcode)

        try:
            with open(cache_fname, 'rb') as f:
                result = pickle.load(f)
        except FileNotFoundError as e:
            print(f"[Error] No cache result found for {task}")
            raise 
        else:
            return result
        

    def save(self, task):

        if task.save_action == MANUAL: return
        cache_fname = join(self.directory, task.hashcode)
        
        # TODO: Do some research on pickle protocols for performance issues
        with open(cache_fname, 'wb') as f:
            pickle.dump(task.result, f, protocol=0)

        self.update()


    def remove(self, task):
        
        cache_fname = join(self.directory, task.hashcode)
        try: 
            remove(cache_fname)
        except FileNotFoundError as e:
            pass

        self.update()