
from constants import AUTO, MANUAL
from os.path import exists

def do_nothing():
    pass

class Task:

    from .GetWorkload import getWorkload
    from .CreateHashCode import createHashCode

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
        self.output_file=output_file

        self.dryrun_passthrough = False

        self.hashcode = self.createHashCode() if save_action == AUTO else None
        self.result = None

        if self.save_action == AUTO:
            self.output_file = self.pipeline.cache.getResultFilename(self)

    def __str__(self):

        if self.pipeline.debug_mode:
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

        if self.pipeline.dryrun_mode:
            return self.dryrun_passthrough

        if self.save_action == AUTO:
            return self.pipeline.cache.hasResultFor(self)
        elif self.save_action == MANUAL:
            return exists(self.output_file)
        else :
            print(f"[Error] task.save_action = {self.save_action}. Check your config file")
            exit()

    def getResult(self):

        if not self.resultExists():
            print("[Warning] A call to getResult returned None")
            return None

        if self.result is not None:
            return self.result
        elif self.save_action == AUTO:
            return self.pipeline.cache.load(self)
        elif self.save_action == MANUAL:
            return self.output_file


    def run(self):

        if self.pipeline.dryrun_mode:
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

        self.pipeline.cache.save(self)
        
            