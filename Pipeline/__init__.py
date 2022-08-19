

class Pipeline:

    from .ParseConfig import parseConfig
    from .Run import run

    def __init__(self, config_file):
        self.parseConfig(config_file)



    


        