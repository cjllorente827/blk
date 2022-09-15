import time
from mpi4py import MPI
from blk.utils import format_time
from blk.constants import ITERATION_LIMIT

def run(self):

    COMM = MPI.COMM_WORLD
    comm_size = COMM.Get_size()
    comm_rank = COMM.Get_rank()
    is_root = comm_rank == 0

    start = time.time()

    self.dryrun_mode and is_root and print("Performing dry run...")

    execution_stack = self.root_task.getWorkload()

    iterations = 0
    while len(execution_stack) > 0 and iterations < ITERATION_LIMIT:
        iterations += 1

        task_list = []
        new_execution_stack = []

        for task in execution_stack:
            if task.isReady():
                is_root and print(f"[{iterations}] Ready to execute: {str(task)}")
                task_list.append(task)
            else :
                is_root and print(f"[{iterations}] Delaying execution of: {str(task)}")
                new_execution_stack.append(task)
        # end for task


        # Run the next batch of tasks and save them to the cache
        for task in task_list:
            COMM.Barrier() # make sure every process is running on the same task
            is_root and print(f"[{iterations}] Running: {str(task)}")
            task.run()
        # end for task

        # update the execution stack with the remaining work
        execution_stack = new_execution_stack
    # end while

    self.runtime = format_time(time.time() - start)
    print(f"Total runtime: {self.runtime}")
        