import os

from blk import cache
from blk.enums import AUTO, MANUAL


class Stage():

    """
    A Stage represents a single unit of pipeline execution. 

    A Stage may or may not depend on the execution of previous
    stages. These dependencies are stored as a list of other Stage objects.
    Before executing itself, a Stage checks to see if its dependencies 
    have cached results already available, if not, it executes its dependencies, 
    which will in turn execute their dependencies in a recursive manner. 

    operation   --  The function that performs the actions required to produce a result
                    that can be saved to the cache and referenced later, or to make
                    a final analysis product, i.e. a plot. 

    arguments --    List of arguments passed to the operation function. These arguments get
                    included in the hash if this stage's result_action is "auto". Other arguments 
                    to the function, i.e. those from dependencies, do not get included
                    in the hash. 

    tag --          User-defined tag for the stage so it can be determined by a human what data
                    each result actually corresponds to. Used as the internal id for the Stage
                    if action set to MANUAL

    depends_on --   List of Stages that must be completed prior to this Stage's execution.
                    If None, this stage can be executed independently of any others and 
                    thus represents a leaf node in the dependency tree. 
    
    action     --   "auto" if this stage should automatically handle its result by pickling
                        the data and placing it in the cache, or "manual" if the operation itself
                        will handle the creation of any necessary intermediate files

    force_execute  --   If set to True, this Stage will execute regardless if a previous 
                        result exists

    run_only_on_root -- Only use if running using Stage parallelism. This setting is ignored
                        otherwise. Ensures this stage runs serially on the root process only.
    """

    def __init__(self, 
        operation, 
        arguments, 
        tag,
        depends_on=[], 
        action=AUTO,
        run_only_on_root=False, 
        force_execute=False):

        self.operation = operation
        self.arguments = arguments
        self.tag = tag
        self.action = action
        self.force_execute = force_execute
        self.run_only_on_root = run_only_on_root
        self.is_gather = False
        
        self.dependencies = dict()
        for stage in depends_on:
            self.dependencies[stage.cache_id] = stage


        # The cache id is a very important internal id for tracking
        # individual stages and their completion, each cache_id
        # within a specific pipeline should be unique
        #
        # TODO: 
        # It would be a good idea to introduce a mechanism to enforce
        # this instead of just allowing the user to introduce 
        # a bunch of stages with the same tag.
        self.cache_id = ""
        if self.action == AUTO:
            self.cache_id = cache.get_hash(self.operation, self.arguments)
        else:
            self.cache_id = tag

    def __str__(self):
        return f"{self.operation.__name__} {self.tag}"

    def __repr__(self):
        return str(self)
    

def gather(package_name, stages,compress):

    try:
        #print(f"mkdir {package_name}")
        os.mkdir(package_name)
    except FileExistsError as e:
        pass

    for stage in stages:
        
        file_name = os.path.join(cache.CACHE_DIR, stage.cache_id)
        package_loc = os.path.join(package_name, stage.tag)
        cmd = f"cp {file_name} {package_loc}"

        #print(cmd)
        os.system(cmd)

    if compress:
        cmd = f"tar cvzf {package_name}.tar.gz {package_name}"
        #print(cmd)
        os.system(cmd)

class Gather(Stage):

    def __init__(self, package_name, stages, compress=False):
        super().__init__(
            gather,
            [package_name, stages,compress],
            package_name,
            depends_on=stages,
            action=MANUAL,
            force_execute=True,
            run_only_on_root=True
        )

        self.is_gather = True