
from blk.Tasks import Task
from blk.constants import MANUAL


def finalize(args, pipeline=None):
    pipeline.writePipelineInfo()

class Terminal(Task):

    def __init__(self, pipeline):

        output_file = f"{pipeline.config_file}.out"
        arguments = {
            "pipeline" : pipeline
        }
        
        super().__init__(
            operation=finalize,
            arguments=arguments,
            pipeline=pipeline,
            dependencies=pipeline.all_tasks[-1],
            save_action=MANUAL,
            always_run=True,
            output_file=output_file
        )