

from os.path import abspath, exists
from os import mkdir, getenv
import importlib

from blk.constants import (
    MANUAL,
    MAX_SEGMENTS,
    ONE_TO_ONE,  
    NONE,
    AUTO,
    TASK_ACTION_OPTIONS
)

from blk import Task, Terminal, Cache
from mpi4py import MPI


# the special arguments that get used in the creation of tasks for a given segment
# any parameters that do not match these are supplied as keyword arguments for 
# the task operation
SPECIAL_ARGS = [
    "operation",
    "name",
    "num_tasks",
    "dependency_strategy",
    "format",
    "format_start_index",
    "save_action",
    "always_run"
]

def parseConfig(self, config):

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()

    self.cache_dir = abspath(config["blk"]["cache_dir"])
    self.operations_module = importlib.import_module(config["blk"]["operations_module"])

    if "debug_mode" in config["blk"].keys():
        self.debug_mode = config.getboolean("blk", "debug_mode") and comm_rank == 0

    if "dryrun_mode" in config["blk"].keys():
        self.dryrun_mode = config.getboolean("blk","dryrun_mode")


    self.cache = Cache(self.cache_dir)

    i = 1
    while f"segment {i}" in config.sections() and i < MAX_SEGMENTS: 
        i += 1

    self.num_segments = i-1
    
    self.debug_mode and print(f"{self.num_segments} pipeline segments detected")

    self.all_tasks = []

    i = 0
    for i in range(self.num_segments):

        self.all_tasks += [[]]

        current_segment = f"segment {i+1}"

        # determine the number of tasks in this segment
        dependency_strategy = config[current_segment]["dependency_strategy"] \
            if "dependency_strategy" in config[current_segment].keys() \
            else NONE

        if dependency_strategy == ONE_TO_ONE:
            num_tasks = len(self.all_tasks[i-1])
        else :
            num_tasks = config.getint(current_segment, "num_tasks")\
            if "num_tasks" in config[current_segment].keys() \
            else 1

        if num_tasks == 0:
            print(f"[Error] {current_segment} was created with 0 tasks! Check your config file.")
            exit()

        # determine whether blk handles the output files (AUTO) or if the 
        # operation function will do that (MANUAL)
        if "save_action" in config[current_segment].keys():
            save_action = config[current_segment]["save_action"] 
            assert(save_action in TASK_ACTION_OPTIONS)
        else:
            save_action = AUTO

        # grab the operation function pointer
        operation = getattr( self.operations_module, config[current_segment]["operation"])

        # determine if any format strings need to be created
        format_args = []
        format_strings = []
        format_start_index = 0
        if "format" in config[current_segment].keys():
            format_args = [a.strip() for a in config[current_segment]["format"].split(',')]
            format_strings = [config[current_segment][a] for a in format_args]

            if "format_start_index" in config[current_segment].keys():
                format_start_index = config.getint(current_segment, "format_start_index")
            

        # TODO: An always run task should clear its cached result before 
        # the pipeline starts executing
        always_run = False
        if "always_run" in config[current_segment].keys():
            always_run = self.guessType(config[current_segment]["always_run"])

        dependencies_list = self.getDependencies(dependency_strategy, num_tasks)

        for j in range(num_tasks):

            # if any arguments need to be formatted, do that here
            for k, a in enumerate(format_args):
                config[current_segment][a] = format_strings[k].format(format_start_index+j)

            # if using the manual save action, determine the name of the output file
            # make sure this is done after formatting to ensure that the output files
            # can be formatted with the task index number

            output_file = config[current_segment]["output_file"] \
                if save_action == MANUAL\
                else None 

            name = config[current_segment]["name"] \
                if "name" in config[current_segment].keys()\
                else None

            # collect all the other properties and convert them to keyword arguments
            arguments = {}
            for key in config[current_segment].keys():

                if  key in SPECIAL_ARGS : continue
                arguments[key] = self.guessType(config[current_segment][key])

                # If this is a function, replace its value with the function's 
                # return value 
                if callable( arguments[key]) :
                    f = arguments[key]
                    arguments[key] = f(j, num_tasks)
            # end for key
            
            # create the Task
            new_task = Task(
                name=name,
                pipeline=self,
                operation=operation,
                arguments=arguments,
                index=(j if num_tasks > 1 else None),
                dependencies=dependencies_list[j],
                save_action=save_action,
                output_file=output_file,
                always_run=always_run
            )
            self.all_tasks[i].append(new_task)
        # end for j
        self.debug_mode and print(f"Segment {i+1} created with {num_tasks} tasks")
    # end for i

    
    
    term = Terminal(self)
    self.all_tasks += [[]]
    self.all_tasks[-1].append(term)

    self.root_task = term

        
boolean_values = {
    "True"  : True,
    "False" : False,
    "true"  : True,
    "false" : False,
    "on"    : True,
    "off"   : False,
    "yes"   : True,
    "no"    : False
}


# tries to guess what the type of the value is based on its string representation
# will return whatever it guesses
def guessType(self, value):

    # trim quotes and white space from string values
    value = value.strip()
    value = value.strip('"')

    # try the most restrictive types first
    if value in boolean_values.keys():
        return boolean_values[value]

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass

    try:
        return complex(value)
    except ValueError:
        pass

    def dressAsArray(value):

        if not value.startswith('['):
            value = '['+value

        if not value.endswith(']'):
            value = value+']'

        return value

    # try converting it to an array
    if (value.startswith('[') and value.endswith(']')):

        # test for multi-dimensionality
        multi_values = value[1:-1].split('],[')
        multidim = len(multi_values) > 1
        if multidim:
            values = [ dressAsArray(v) for v in multi_values]
        else : 
            values = value[1:-1].split(',')

        arr = []
        for v in values:
            arr.append( self.guessType(v) )

        return arr

    # or a tuple
    if (value.startswith('(') and value.endswith(')')):
        values = value[1:-1].split(',')

        arr = []
        for v in values:
            arr.append( self.guessType(v) )

        return tuple(arr)

    # maybe its defined in our operations module
    try:
        attr = getattr(self.operations_module, value)
    except AttributeError as e:
        pass
    else:
        return attr

    # TODO: See if its a file 

    # Give up and assume its a string
    return value


