

from Pipeline import Pipeline


class TaskParallelPipeline(Pipeline):

    from .TaskParallelRun import run

    def __init__(self, config_file):
        super().__init__(config_file)