
from blk.Pipeline import Pipeline


class SegmentParallelPipeline(Pipeline):

    from .SegmentParallelRun import run

    def __init__(self, config_file):
        super().__init__(config_file)