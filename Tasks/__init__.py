
from blk.constants import AUTO, MANUAL
from os.path import exists
from sys import exit

def do_nothing():
    pass

class Task:

    from .GetWorkload import getWorkload
    from .CreateHashCode import createHashCode
    from .WidgetFunctions import (
        createWidget, 
        displayWidgets, 
        setArgumentsFromWidgets,
        createSaveAndRunButton
    )

    def __init__(
        self, 
        name=None,
        pipeline=None,
        cache=None,
        operation=do_nothing, 
        arguments=None, 
        index=None,
        dependencies=None, 
        save_action=AUTO,
        output_file=None,
        always_run=False ):

        if name == None:
            if index != None:
                self.name = f"{operation.__name__}[{index}]"
            else: 
                self.name = operation.__name__
        else :
            if index != None:
                self.name = f"{name}[{index}]"
            else: 
                self.name = name

        has_pipeline = pipeline != None
        self.has_dependencies = dependencies != None and len(dependencies) > 0

        self.dependencies = dependencies if self.has_dependencies else None

        self.pipeline = pipeline

        # prioritize using the pipeline's cache
        self.cache = pipeline.cache if has_pipeline else cache

        # if that doesn't work, try grabbing the cache from our dependencies
        if self.cache == None and self.has_dependencies:
            self.cache = dependencies[0].cache

        # if all that failed then give up
        if self.cache == None:
            print("[Error] Task cannot be created without a Cache object. Exiting.")
            exit()

        # if no arguments were given, try copying the arguments from a dependency
        if arguments == None and self.has_dependencies:
            self.arguments = dependencies[0].arguments.copy()
        # otherwise, initialize with an empty dict
        elif arguments == None:
            self.arguments = {}

        self.operation = operation
        self.index = index
        self.save_action = save_action
        
        self.always_run = always_run
        self.result = None

        self.dryrun_passthrough = False

        # Handle setting the hashcode and the output_file 
        self.hashcode = None
        self.output_file = None

        if save_action == AUTO and len(self.arguments.keys()) > 0: 
            self.createHashCode()


        if self.save_action == MANUAL:
            if 'output_file' in self.arguments:
                self.output_file = arguments["output_file"]
            else:
                self.output_file = "blk_default.out"

        self.widget_list = {}

    def __str__(self):

        if self.pipeline != None and self.pipeline.debug_mode:
            my_str = f"{self.name}\nHashcode: {self.hashcode}\n"
            
            if self.dependencies != None and len(self.dependencies) > 0:
                my_str += "Depends on:\n"
                for d in self.dependencies:
                    my_str += f"\t{d.name}\n"
            else:
                my_str += "No dependencies\n"

            my_str += f"""
            Always run : {self.always_run}
            Save action: {self.save_action}
            Output File: {self.output_file}"""
        elif self.hashcode != None:
            my_str = f"{self.name} {self.hashcode}"
        else : 
            my_str = f"{self.name}"

        return my_str

    def __repr__(self):
        my_str = f"{self.name}\nHashcode: {self.hashcode}\n"

        if self.dependencies != None and len(self.dependencies) > 0:
            my_str += "Depends on:\n"
            for d in self.dependencies:
                my_str += f"\t{d.name}\n"
        else:
            my_str += "No dependencies\n"

        my_str += f"""Always run : {self.always_run}
Save action: {self.save_action}
Output File: {self.output_file}"""

        return my_str


    def setArguments(self, **kwargs):

        for k,v in kwargs.items():
            self.arguments[k] = v

        if self.save_action == MANUAL and "output_file" in kwargs:
            self.output_file = kwargs["output_file"]

        self.save_action == AUTO and self.createHashCode()

    # Figure out if this task needs to be run or not
    # A task needs to be run if
    #   task.always_run == True
    #            OR
    #   the cache has no result for this task
    def mustBeRun(self):
        return self.always_run or not self.resultExists()

    # A task is ready to be run if there exists a result for 
    # all of its dependencies
    def isReady(self):
        
        if self.dependencies == None:
            return True

        for dep in self.dependencies:
            if not dep.resultExists():
                return False
        return True
        

    def resultExists(self):

        if self.pipeline != None and self.pipeline.dryrun_mode:
            return self.dryrun_passthrough

        if self.save_action == AUTO:
            return self.cache.hasResultFor(self)
        elif self.save_action == MANUAL:
            return exists(self.output_file)
        else :
            print(f"[Error] task.save_action = {self.save_action}. Check your config file")
            exit()

    def getResult(self):

        if not self.resultExists() or self.always_run:

            if self.pipeline != None:
                print("[Warning] A call to getResult returned None")
                return None

            if self.isReady():
                print(f"Running: {str(self)}")
                self.run()
            else:
                print(f"[Error] {str(self)} has unmet dependencies. Run dependencies before attempting to retrieve a result. ")
                exit()

        if self.result is not None:
            return self.result
        elif self.save_action == AUTO:
            return self.cache.load(self)
        elif self.save_action == MANUAL:
            return self.output_file


    def run(self):

        if self.pipeline != None and self.pipeline.dryrun_mode:
            self.dryrun_passthrough = True
            return 

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

        self.cache.save(self)
        
            