# Operation function that collects data about the pipeline it is told to run
# and outputs it to file for debugging/profiling etc.

from tabulate import tabulate

def writePipelineInfo(self):

    headers = {
        "Segment" : "segment",
        "Name" : "name",
        "Hashcode" : "hashcode",
        "Dependencies" : "dependencies",
        "Save Action" : "save_action",
        "Always run" : "always_run",
        "Output File" : "output_file",
    }

    output = f"Pipeline created from {self.config_file}\nRuntime: {self.runtime}\n"
    output += f"Cache set to : {self.cache.directory}\n"

    table = {}
    for k in headers.keys():
        table[k] = []
        for i, segment in enumerate(self.all_tasks):
            for task in segment:
                
                if k == "Dependencies":
                    if task.dependencies is not None:
                        dep_string = '\n'.join([dep.name for dep in task.dependencies])
                        table[k].append(dep_string)
                    else:
                        table[k].append("None")
                elif k == "Segment":
                    table[k].append(f"{i+1}")
                else:
                    table[k].append( getattr(task, headers[k]) )

            # end for k
        # end for task
    # end for i, segment
    output += tabulate(table, headers="keys") + '\n\n'

    with open(f"{self.config_file}.out", 'w+') as f:
        f.write(output)

    