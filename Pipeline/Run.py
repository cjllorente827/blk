import time
from utils import format_time
from constants import ITERATION_LIMIT

def run(self):

    start = time.time()

    execution_stack = self.root_task.getWorkload()

    iterations = 0
    while len(execution_stack) > 0 and iterations < 5:
        iterations += 1

        task_list = []
        new_execution_stack = []

        for task in execution_stack:
            if task.isReady():
                self.debug_mode and print(f"[{iterations}]Ready to execute: {str(task)}")
                task_list.append(task)
            else :
                self.debug_mode and print(f"[{iterations}]Delaying execution of: {str(task)}")
                new_execution_stack.append(task)
        # end for task


        # Run the next batch of tasks and save them to the cache
        for task in task_list:
            task.run()
            self.cache.save(task)
        # end for task

        # update the execution stack with the remaining work
        execution_stack = new_execution_stack
    # end while

    runtime = format_time(time.time() - start)
    print(f"Total runtime: {runtime}")
        