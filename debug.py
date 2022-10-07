
from blk import ProjectionTask, ProjectionPlotTask, Cache


my_cache = Cache()

proj = ProjectionTask(cache=my_cache)


plot = ProjectionPlotTask(dependencies=[proj])
