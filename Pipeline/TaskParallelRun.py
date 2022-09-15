
from mpi4py import MPI
import time
from blk.utils import format_time
from blk.constants import ITERATION_LIMIT

def run(self):

    COMM = MPI.COMM_WORLD
    comm_size = COMM.Get_size()
    comm_rank = COMM.Get_rank()
    is_root = comm_rank == 0
    
    start = time.time()

    self.dryrun_mode and is_root and print("Performing dry run...")
    COMM.Barrier()

    execution_stack = None
    task_list = None
    execution_stack_size = 0
    
    execution_stack = self.root_task.getWorkload()
    execution_stack_size = len(execution_stack)
    
    iterations = 0
    while execution_stack_size > 0 and iterations < 10:
        
        iterations += 1

        task_list = []
        new_execution_stack = []
        proc = 0

        self.cache.update()

        for task in execution_stack:
            if task.isReady() :
                if self.dryrun_mode:
                    task.dryrun_passthrough = True
                if proc == comm_rank:
                    print(f"[{iterations}][Rank {comm_rank}] Ready to execute: {str(task)}")

                    # assign tasks with round robin
                    task_list.append(task)
                proc = proc+1 if proc+1 < comm_size else 0
            else :
                is_root and print(f"[{iterations}][Rank {comm_rank}] Delaying execution of: {str(task)}")
                new_execution_stack.append(task)
        # end for task

        COMM.Barrier()
        # Run the next batch of tasks and save them to the cache
        for task in task_list:
            print(f"[{iterations}][Rank {comm_rank}] Running: {str(task)}")
            task.run()
        # end for task
        COMM.Barrier()

        execution_stack = new_execution_stack
        execution_stack_size = len(execution_stack)

    # end while

    self.runtime = format_time(time.time() - start)
    is_root and print(f"Total runtime: {self.runtime}")