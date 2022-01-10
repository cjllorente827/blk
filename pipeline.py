import time, os, sys, argparse
from mpi4py import MPI
import blk.utils

# enumerate possible values for result_action
ACTION_OPTIONS = ["auto", "manual"]
AUTO, MANUAL = ACTION_OPTIONS

# enumerate possible values for parallelism
PARALLELISM_OPTIONS = ["none", "task", "stage"]
NONE, TASK, STAGE = PARALLELISM_OPTIONS

TASK_STATUS_OPTIONS = ["complete", "skipped", "failed"]
COMPLETE, SKIP, FAIL = TASK_STATUS_OPTIONS


DEBUG = False
DRYRUN = False

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

    run_only_on_root -- Only use if running using Stage parallelism. This setting is ignored
                        otherwise. Ensures this stage runs serially on the root process only.
    """

    def __init__(self, 
        operation, 
        arguments, 
        tag="root",
        depends_on=[], 
        result_action=AUTO,
        result_id=None,
        run_only_on_root=False, 
        force_execute=False):

        self.operation = operation
        self.arguments = arguments
        self.result_action = result_action
        self.force_execute = force_execute
        self.tag = tag
        self.run_only_on_root = run_only_on_root
        
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
        return f"{self.operation.__name__} {self.arguments[0]}"

    def __repr__(self):
        return str(self)
    
#     def __repr__(self):
#         dependency_tags = ""
#         for result_id, stage in self.dependencies.items():
#             dependency_tags += stage.tag + "\n    "

#         return f"""
# Stage: {self.operation.__name__} 
# arguments: {self.arguments}
# tag: {self.tag}
# depends on: 
#     {dependency_tags}
#         """

"""
Runs the stage, saves result to cache if set to AUTO
"""    
def run(task, parallelism):

    if parallelism == STAGE and task.run_only_on_root and RANK != 0:
        return SKIP

    if COMM_SIZE > 1:
        if DEBUG: print(f"Rank {RANK} running task: {task}")
    else:
        if DEBUG: print(f"Running task: {task}")

    if DRYRUN: return COMPLETE

    if len(task.dependencies) == 0:
        args = task.arguments

    else: 
        data_dict = dict()
        for result_id, stage in task.dependencies.items():
            if stage.result_action == AUTO:
                data_dict[stage.tag] = blk.utils.load_result_from_cache(result_id)

        if len(data_dict) > 0:
            args = [data_dict, *task.arguments]
        else:
            args = task.arguments

    data = task.operation(*args)

    # Figure out if we're saving to file automatically
    # Stage-parallel tasks only save on the root process
    x = task.result_action == AUTO 
    y = parallelism == STAGE
    z = RANK == 0
    do_save =  x and (not y or z)
    if do_save:
        if DEBUG: print(f"Saved results of {task} to file with id: {task.result_id}")
        blk.utils.save_to_cache(data, task.result_id)

    if DEBUG: print(f"Finished {task}")
    return COMPLETE # finished with no errors
    
    

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

    global DEBUG, DRYRUN
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action="store_true", default=False)
    parser.add_argument('-y', action="store_true", default=False)
    
    args, unknown = parser.parse_known_args()
    DEBUG = args["d"] or args["y"]
    DRYRUN = args["y"]
    

    if parallelism not in PARALLELISM_OPTIONS:
        print(f"Invalid option for parallelism: {parallelism}")
        return False

    start = time.time()

    # Only the root process should be determining the workload
    # The root process is the source of truth for what needs to 
    # be done and what is finished
    if RANK == 0:
        execution_stack, cache = determine_workload(root_stage)
        execution_stack_size = len(execution_stack)
    else: 
        execution_stack_size = 0
        cache = set()

    execution_stack_size = COMM.bcast(execution_stack_size, root=0)

    iterations = 0
    while execution_stack_size > 0 and iterations < ITERATION_LIMIT:
        iterations += 1

        if RANK == 0:
            task_list = collect_task_list(execution_stack, cache, 
                parallelism=parallelism, 
                num_procs=COMM_SIZE)

            if DEBUG:
                if parallelism == TASK:   
                    print(f"\nIteration {iterations} task list:")
                    for i, tl in enumerate(task_list):
                        print(f"\n\tRank {i}")
                        for task in tl:
                            print(f"\t{task}")
                else: 
                    for task in task_list: print(f"\t{task}")


        else: 
            task_list = None
            cache = set() # make sure we have an empty cache on non-root processes each loop

        if parallelism == STAGE:
            task_list = COMM.bcast(task_list, root=0)
            
        elif parallelism == TASK:
            task_list = COMM.scatter(task_list, root=0)

        for task in task_list:

            # Actually run the task
            task_status = run(task, parallelism)

            if task_status == COMPLETE:
                cache.add(task.result_id)
            elif task_status == SKIP:
                if DEBUG: print(f"Rank {RANK} skipped task {task}")
            elif task_status == FAIL:
                print(f"Task failed: {task}")
        # end for task


        # Gather results from other processes about the tasks they completed
        # and save them to the cache
        if parallelism == TASK:
            all_caches = cache
            all_caches = COMM.gather(all_caches, root=0)

            if RANK == 0: cache = reduce_cache(all_caches)
        
        # Recalculate how much work we have left and broadcast the result to 
        # the other processes
        if RANK == 0:
            new_execution_stack = []
            for task in execution_stack:
                if task.result_id not in cache:
                    new_execution_stack.append(task)

            execution_stack = new_execution_stack
            execution_stack_size = len(execution_stack)
        else:
            execution_stack_size = 0
        
        if parallelism == STAGE or parallelism == TASK:
            execution_stack_size = COMM.bcast(execution_stack_size, root=0)

    # end while 

    runtime = blk.utils.format_time(time.time() - start)
    if RANK == 0: print(f"Total runtime: {runtime}")
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

            do_stage = stage.force_execute or \
                (not blk.utils.exists_in_cache(stage) and \
                not os.path.exists(stage.result_id))

            if do_stage:
                traversal_queue.append(stage)
                execution_stack.append(stage)
            else :
                cache.add(stage.result_id)
                
        traversal_queue.remove(current_stage)
    # end while 

    return execution_stack, cache

            

def collect_task_list(execution_stack, cache, parallelism=NONE, num_procs=1):

    if parallelism == NONE or parallelism == STAGE:
        task_list = []

        for stage in execution_stack:
            if dependencies_are_met(stage, cache):
                task_list.append(stage)
        return task_list
    
    elif parallelism == TASK:

        task_list = [ [] for i in range(num_procs) ]

        all_tasks = []
        for stage in execution_stack:
            if dependencies_are_met(stage, cache):
                all_tasks.append(stage)

        iterations = 0
        while len(all_tasks) > 0 and iterations < ITERATION_LIMIT:
            task_list[iterations % num_procs].append(all_tasks.pop())
            iterations += 1

        return task_list
    return None


def dependencies_are_met(stage, cache):
    all_met = all([result_id in cache for result_id in stage.dependencies.keys()])

    if DEBUG and all_met: 
        print(f"All dependencies met for {stage}")
    elif DEBUG: 
        print(f"Waiting on dependencies for {stage}")

    return all_met


def reduce_cache(cache_list):

    cache = set()
    for c in cache_list:
        cache = cache | c
    return cache


        

        