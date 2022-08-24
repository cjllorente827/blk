# blk

## What is blk?

Blk is an automated pipeline generation code. Let's say you have some python code that you've written to perform analysis on some data and create a plot. 
This process can be generally be split into 3 parts:

 1. Query a dataset to obtain the specific data required for your analysis
 2. Perform the calculations that comprise your analysis (average, max, etc.)
 3. Plot the data on a nice-looking graph

When working with large datasets, these 3 steps are not created equal. Depending on what you're doing, either of the first two steps can run for 
much longer than the plotting step. This can be frustrating since creating publication-ready plots often requires many small changes to individual plots that do not necessarily require re-doing the entire analysis chain. 

This is where blk can help. Blk offers a structured way to split up your analysis pipeline into blocks called **Segments** which are composed of individual units of code execution called **Tasks**. All the data produced by a Task is saved immediately after the Task is run, and blk is clever enough to figure out when a Task has already been completed. That way, if you need to run your analysis pipeline again, blk will retrieve the data you've already created instead of re-running everything!

## How does blk work?

Blk reads in a config file with a ".pipe" extension in order to generate the Tasks and Segments that make up your Pipeline. Here's an example of a pipeline config file:

```
[blk]

# this is where blk saves intermediate data files
cache_dir = ./cache

# more on this later
parallel = none

# dry run mode executes the pipeline without actually running any of the code
# good for testing if the pipeline config file has been written correctly
dryrun_mode = off

# prints out extra information while running to help determine the source of any errors
debug_mode = on

# name of the file where operation definitions are kept
# or 'blk' to use blk premade operations
operations_module = blk


# Every section that describes a segment should be called "segment <N>" where <N> is replaced with the number of that segment. 
[segment 1]

; name of the function this segment will execute
operation = projection

# how many tasks will be created in this segment of the pipeline
num_tasks = 1

enzo_dataset = path/to/dataset/dataset

# the rest of these will be passed in as keyword arguments
field = ("gas", "density")
field_units = g/cm**3

box_center = [0.5, 0.5, 0.5]
box_length = 0.5


[segment 2]

operation = projectionPlot

# determines how dependencies get assigned
# this strategy assigns segment 1 task 1 as a dependency of segment 2 task 1
dependency_strategy = one-to-one

# this value will be overidden due to the dependency strategy
num_tasks = 0

# run this segment, even if it's been run it before
always_run = yes

enzo_dataset = path/to/dataset/dataset

# the operation will handle file output
save_action = manual

# where to save the image file 
output_file = density.png

# the rest of these will be passed in as keyword arguments
field_name = Density
axes_units = Mpccm/h

box_length = 0.5

# colorbar settings
cmap = viridis
zlims = [1e-31, 9e-25]


```






