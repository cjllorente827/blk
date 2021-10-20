import time, os
from mpi4py import MPI
import blk.utils

# enumerate possible values for result_action
ACTION_OPTIONS = ["auto", "manual"]
AUTO, MANUAL = ACTION_OPTIONS

# enumerate possible values for parallelism
PARALLELISM_OPTIONS = ["none", "task", "stage"]
NONE, TASK, STAGE = PARALLELISM_OPTIONS

class Stage():

    """
    A Stage represents a single unit of pipeline execution. 

    A Stage may or may not depend on the execution of previous
    stages. These dependencies are stored as a list of other Stage objects.
    Before executing itself, a Stage checks to see if its dependencies 
    have cached results already available, if not, it executes its dependencies, 
    which will in turn execute their dependencies in a recursive manner. 

    operation   --  The function that performs the actions required to produce a result
                    that can be saved to the cache and referenced later, or to make
                    a final analysis product, i.e. a plot. 

    arguments --    List of arguments passed to the operation function. These arguments get
                    included in the hash if this stage's result_id_type is HASH. Other arguments 
                    to the function, i.e. those from dependencies, do not get included
                    in the hash. 

    tag --          User-defined tag for the stage so it can be determined what data
                    each result actually corresponds to. If not set, assumed to be a 
                    root node (set to "root").

    depends_on --   List of Stages that must be completed prior to this Stage's execution.
                    If None, this stage can be executed independently of any others and 
                    thus represents a leaf node in the dependency tree. 
    
    result_action --   "auto" if this stage should automatically handle its result by pickling
                        the data and placing it in the cache, or "manual" if the operation itself
                        will handle the creation of any necessary intermediate files

    result_id   --  Essentially this is the filename where the result of this stage will be
                    stored. If its raw data, this will be a hash of the operation function
                    and the arguments passed to that function. If its an image or something
                    similar, it will simply be a user-defined filename. Ignored if result_action
                    is set to "auto". Must not be None if set to "manual"

    force_execute  --   If set to True, this Stage will execute regardless if a previous 
                        result exists
    """

    def __init__(self, 
        operation, 
        arguments, 
        tag="root",
        depends_on=[], 
        result_action=AUTO,
        result_id=None, 
        force_execute=False):

        self.operation = operation
        self.arguments = arguments
        self.result_action = result_action
        self.force_execute = force_execute
        self.tag = tag
        
        self.dependencies = dict()
        for stage in depends_on:
            self.dependencies[stage.result_id] = stage

        if self.result_action == AUTO:
            self.result_id = blk.utils.get_func_hash(self.operation, self.arguments)
        elif result_id is None:
            pass
            # TODO: throw some kind of exception
        else:
            self.result_id = result_id

    def __str__(self):
        return self.operation.__name__
    
    def __repr__(self):
        dependency_tags = ""
        for result_id, stage in self.dependencies.items():
            dependency_tags += stage.tag + "\n    "

        return f"""
Stage: {self.operation.__name__} 
arguments: {self.arguments}
tag: {self.tag}
depends on: 
    {dependency_tags}
        """

"""
Runs the stage, saves result to cache if set to AUTO
"""    
def run(stage, parallelism):

    if len(stage.dependencies) == 0:
        args = stage.arguments

    else: 
        data_dict = dict()
        for result_id, stage in stage.dependencies.items():
            if stage.result_action == AUTO:
                data_dict[stage.tag] = blk.utils.load_result_from_cache(result_id)

        if len(data_dict) > 0:
            args = [data_dict, *stage.arguments]
        else:
            args = stage.arguments

    data = stage.operation(*args)

    # Figure out if we're saving to file automatically
    # Stage-parallel tasks only save on the root process
    x = stage.result_action == AUTO 
    y = parallelism == STAGE
    z = RANK == 0
    do_save =  x and (not y or z)
    if do_save:
        blk.utils.save_to_cache(data, stage.result_id)

    return True # finished with no errors
    
    

"""
Executes a pipeline assuming that the stage passed in is the root stage, i.e. has no
dependencies and is the last stage to be executed. 
"""

# This code makes use of several while loops
# While loops are notorious for running forever due to bugs with 
# their loop conditions. This limit ensures that the code
# will eventually complete no matter what
ITERATION_LIMIT = 1e6

COMM = MPI.COMM_WORLD
COMM_SIZE = COMM.Get_size()
RANK = COMM.Get_rank()

def execute(root_stage, parallelism=NONE):

    if parallelism not in PARALLELISM_OPTIONS:
        print(f"Invalid option for parallelism: {parallelism}")
        return False

    start = time.time()

    execution_stack, cache = determine_workload(root_stage)

    iterations = 0
    while len(execution_stack) > 0 and iterations < ITERATION_LIMIT:
        iterations += 1
        task_list = []

        # add the stages whose dependencies are already met to the task list
        for stage in execution_stack:
            if all([result_id in cache for result_id in stage.dependencies.keys()]):
                task_list.append(stage)
                execution_stack.remove(stage)

        if parallelism == NONE or parallelism == STAGE:
            for task in task_list:
                print("Running task:")
                print(task)
                success = run(task, parallelism)
                if success : 
                    print(f"Finished {str(task)}")
                    cache.add(task.result_id)

        elif parallelism == TASK:
            start_index = RANK/COMM_SIZE
            end_index = (RANK+1)/COMM_SIZE

            for task in task_list[start_index, end_index]:
                print(f"Rank {RANK} Running task:")
                print(task)
                success = run(task, parallelism)
                if success : 
                    print(f"Finished {str(task)}")
                    cache.add(task.result_id)
                else:
                    print("Task failed")
                    print(task)

            COMM.Barrier()
    # end while 

    elapsed = time.time() - start
    print(blk.utils.format_time(elapsed))
    return True

    

def determine_workload(root):

    traversal_queue = []
    execution_stack = []
    cache = set()

    # We always assume we are running the root node in the tree
    execution_stack.append(root)

    # initialize the traversal queue to find any dependencies that
    # need to be run
    traversal_queue.append(root)

    # Do a breadth-first traversal to add all incomplete
    # stages to the execution stack
    iterations = 0
    while len(traversal_queue) > 0 and iterations < ITERATION_LIMIT:
        iterations += 1

        current_stage = traversal_queue[0]
        # go through the stage dependencies 
        # any that have to be executed get added to the traversal queue
        # and execution stack simultaneously
        # otherwise, we simply save the found result in the
        # results dictionary
        for result_id, stage in current_stage.dependencies.items():
            if os.path.exists(stage.result_id):
                cache.add(stage.result_id)
            else :
                traversal_queue.append(stage)
                execution_stack.append(stage)


        traversal_queue.remove(current_stage)
    # end while 

    return execution_stack, cache

            




        

        

        