
from constants import ITERATION_LIMIT

def getWorkload(self):

    traversal_queue = []
    execution_stack = []

    # We always assume we are running the root node in the tree
    execution_stack.append(self)

    # initialize the traversal queue to find any dependencies that
    # need to be run
    traversal_queue.append(self)

    # Do a breadth-first traversal to add all incomplete
    # stages to the execution stack
    iterations = 0
    while len(traversal_queue) > 0 and iterations < ITERATION_LIMIT:
        iterations += 1

        current_stage = traversal_queue[0]
        traversal_queue.remove(current_stage)

        # if this task has any dependencies, add them to the traversal queue
        # unless they have results in the cache
        if current_stage.dependencies == None \
            or len(current_stage.dependencies) < 1: continue
        for task in current_stage.dependencies:

            # If the task has been added by a previous task, skip it
            if task in execution_stack:
                continue

            if task.mustBeRun():
                traversal_queue.append(task)
                execution_stack.append(task)
                
        
    # end while 

    return execution_stack