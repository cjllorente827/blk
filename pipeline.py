import time, os, sys, argparse
from mpi4py import MPI
from blk import cache, utils
from blk.enums import *
from blk.stages import *

DEBUG = False
DRYRUN = False

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

    if len(task.dependencies) == 0 or task.is_gather:
        args = task.arguments
    elif task.operation == utils.noop:
        args = []
    else: 
        data_dict = dict()
        for cache_id, stage in task.dependencies.items():
            if stage.action == AUTO:
                data_dict[stage.tag] = cache.load(cache_id)

        if len(data_dict) > 0:
            args = [data_dict, *task.arguments]
        else:
            args = task.arguments

    data = task.operation(*args)

    # Figure out if we're saving to file automatically
    # Stage-parallel tasks only save on the root process
    x = task.action == AUTO 
    y = parallelism == STAGE
    z = RANK == 0
    do_save =  x and (not y or z)
    if do_save:
        if DEBUG: print(f"Saved results of {task} to file: {task.cache_id}")
        cache.save(data, task.cache_id)

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

def execute(root_stage, parallelism=NONE, debug_mode=False, dry_run=False):

    global DEBUG, DRYRUN
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action="store_true", default=False)
    parser.add_argument('-y', action="store_true", default=False)
    
    args, unknown = parser.parse_known_args()
    args = vars(args)
    DEBUG = args["d"] or args["y"] or debug_mode or dry_run
    DRYRUN = args["y"] or dry_run
    

    if parallelism not in PARALLELISM_OPTIONS:
        print(f"Invalid option for parallelism: {parallelism}")
        return False

    start = time.time()

    # Only the root process should be determining the workload
    # The root process is the source of truth for what needs to 
    # be done and what is finished
    if RANK == 0:
        execution_stack, virtual_cache = determine_workload(root_stage)
        execution_stack_size = len(execution_stack)
    else: 
        execution_stack_size = 0
        virtual_cache = set()

    execution_stack_size = COMM.bcast(execution_stack_size, root=0)

    iterations = 0
    while execution_stack_size > 0 and iterations < ITERATION_LIMIT:
        iterations += 1

        if RANK == 0:
            task_list = collect_task_list(execution_stack, virtual_cache, 
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
            virtual_cache = set() # make sure we have an empty cache on non-root processes each loop

        if parallelism == STAGE:
            task_list = COMM.bcast(task_list, root=0)
            
        elif parallelism == TASK:
            task_list = COMM.scatter(task_list, root=0)

        for task in task_list:

            # Actually run the task
            task_status = run(task, parallelism)

            if task_status == COMPLETE:
                virtual_cache.add(task.cache_id)
            elif task_status == SKIP:
                if DEBUG: print(f"Rank {RANK} skipped task {task}")
            elif task_status == FAIL:
                print(f"Task failed: {task}")
        # end for task


        # Gather results from other processes about the tasks they completed
        # and save them to the cache
        if parallelism == TASK:
            all_caches = virtual_cache
            all_caches = COMM.gather(all_caches, root=0)

            if RANK == 0: virtual_cache = reduce_cache(all_caches)
        
        # Recalculate how much work we have left and broadcast the result to 
        # the other processes
        if RANK == 0:
            new_execution_stack = []
            for task in execution_stack:
                if task.cache_id not in virtual_cache:
                    new_execution_stack.append(task)

            execution_stack = new_execution_stack
            execution_stack_size = len(execution_stack)
        else:
            execution_stack_size = 0
        
        if parallelism == STAGE or parallelism == TASK:
            execution_stack_size = COMM.bcast(execution_stack_size, root=0)

    # end while 

    runtime = utils.format_time(time.time() - start)
    if RANK == 0: print(f"Total runtime: {runtime}")
    return True

    

def determine_workload(root):

    traversal_queue = []
    execution_stack = []

    # the virtual cache is how blk keeps track of what's in the real cache
    # cache = module that maintains functions that change the state of the real cache
    # virtual_cache = data structure that mimics the state of the real cache
    #                 for convenience sake
    virtual_cache = set()

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
        for cache_id, stage in current_stage.dependencies.items():

            # If the stage has been added already, skip it
            if stage in execution_stack:
                continue

            do_stage = stage.force_execute or \
                (not cache.exists(stage) and \
                not os.path.exists(stage.cache_id))

            if do_stage:
                traversal_queue.append(stage)
                execution_stack.append(stage)
            else :
                virtual_cache.add(stage.cache_id)
                
        traversal_queue.remove(current_stage)
    # end while 

    return execution_stack, virtual_cache

            

def collect_task_list(execution_stack, virtual_cache, parallelism=NONE, num_procs=1):

    if parallelism == NONE or parallelism == STAGE:
        task_list = []

        for stage in execution_stack:
            if dependencies_are_met(stage, virtual_cache):
                task_list.append(stage)
        return task_list
    
    elif parallelism == TASK:

        task_list = [ [] for i in range(num_procs) ]

        all_tasks = []
        for stage in execution_stack:
            if dependencies_are_met(stage, virtual_cache):
                all_tasks.append(stage)

        iterations = 0
        while len(all_tasks) > 0 and iterations < ITERATION_LIMIT:
            task_list[iterations % num_procs].append(all_tasks.pop())
            iterations += 1

        return task_list
    return None


def dependencies_are_met(stage, virtual_cache):
    all_met = all([cache_id in virtual_cache for cache_id in stage.dependencies.keys()])

    if DEBUG and all_met: 
        print(f"All dependencies met for {stage}")
    elif DEBUG: 
        print(f"Waiting on dependencies for {stage}")

    return all_met


def reduce_cache(virtual_cache_list):

    virtual_cache = set()
    for c in virtual_cache_list:
        virtual_cache = virtual_cache | c
    return virtual_cache


        

        