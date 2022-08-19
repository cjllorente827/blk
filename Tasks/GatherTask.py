from Tasks import Task
from enums import MANUAL

def gather(task_list=[]):
    pass

class GatherTask(Task):
    def __init__(self, dependencies):
        super().__init__(
            operation=gather,
            arguments={"task_list" : dependencies},
            dependencies=dependencies,
            save_action=MANUAL,
            always_run=True
        )