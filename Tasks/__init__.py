
import inspect, hashlib
from constants import AUTO
from os.path import exists

def do_nothing():
    pass

class Task:

    from .GetWorkload import getWorkload

    def __init__(
        self, 
        pipeline=None,
        operation=do_nothing, 
        arguments={}, 
        index=None,
        dependencies=None, 
        save_action=AUTO,
        output_file=None,
        always_run=False ):

        if index != None:
            self.name = f"{operation.__name__}[{index}]"
        else: 
            self.name = operation.__name__

        self.pipeline = pipeline
        self.operation = operation
        self.arguments = arguments
        self.index = index
        self.save_action = save_action
        self.dependencies = dependencies
        self.always_run = always_run
        self.output_file=output_file,

        self.hashcode = self.createHashCode()
        self.result = None

        if self.save_action == AUTO:
            self.output_file = None
        # TODO: Finish this
        else:
            self.output_file = None

    def __str__(self):
        my_str = f"{self.name}\nHashcode: {self.hashcode}\n"
        
        if self.dependencies != None and len(self.dependencies) > 0:
            my_str += "\tDepends on:\n"
            for d in self.dependencies:
                my_str += f"\t\t{d.name}\n"
        else:
            my_str += "\tNo dependencies\n"

        return my_str


    # Figure out if this task needs to be run or not
    # A task needs to be run if
    #   task.always_run == True
    #            OR
    #   the cache has no result for this task
    def mustBeRun(self):
        return self.always_run or not self.pipeline.cache.hasResultFor(self)

    # A task is ready to be run if there exists a cache result for 
    # all of its dependencies
    def isReady(self):
        
        if self.dependencies == None:
            return True

        for dep in self.dependencies:
            if not dep.hasCachedResult():
                return False
        return True

    def hasCachedResult(self):
        return self.pipeline.cache.hasResultFor(self)


    def getResult(self):
        if self.result != None:
            return self.result
        elif self.hasCachedResult():
            return self.pipeline.cache.load(self)
        elif exists(self.output_file):
            return self.output_file
        else:
            print(f"[Warning] Call to getResult returned None")
            return None


    def createHashCode(self):

        # get the source code of the operation
        source = inspect.getsource(self.operation)

        def remove_all_whitespace(str):
            ws_chars = [' ', '\t', '\n']
            for char in ws_chars:
                str = str.replace(char, '')
            return str

        def append_args(target, args):
            for a in args.values():
                target += str(a)
            return target

        # scrap the whitespace to prevent unnecessary 
        # re-queries
        source_no_ws = remove_all_whitespace(source)

        # concatenate it with the arguments
        target = append_args(source_no_ws, self.arguments)
        
        # convert string to a hash
        return hashlib.md5(target.encode()).hexdigest()

    def run(self):

        if self.dependencies == None or len(self.dependencies) == 0:

            self.result = self.operation(**self.arguments)

        elif len(self.dependencies) == 1:

            result = self.dependencies[0].getResult()
            self.result = self.operation(result, **self.arguments)

        elif len(self.dependencies) > 1:

            results = {}
            for d in self.dependencies:
                results[d.name] = d.getResult()

            self.result = self.operation(results, **self.arguments)
        
            