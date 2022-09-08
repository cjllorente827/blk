from configparser import ConfigParser, ExtendedInterpolation

class Pipeline:

    from .ParseConfig import parseConfig, guessType
    from .Run import run
    from .WritePipelineInfo import writePipelineInfo
    from .GetDependencies import getDependencies

    def __init__(self, config_file):

        config = ConfigParser(
        allow_no_value=True, 
        interpolation=ExtendedInterpolation())

        # prevents configparser from lowercasing everything
        config.optionxform = str
        config.read(config_file)

        self.config_file = config_file
        self.all_tasks = None
        self.runtime = None
        self.cache_dir = None
        self.operations_module = None

        self.debug_mode = False
        self.dryrun_mode = False

        self.cache = None
        self.num_segments = 0
        self.root_task = None
        self.run_time = None

        self.parseConfig(config)



    


        