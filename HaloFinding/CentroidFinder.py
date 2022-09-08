
import yt
import numpy as np

from blk.constants import ITERATION_LIMIT

MAX_ITERATIONS = 25

def centroidFinder(
    enzo_dataset=None,
    initial_guess=[0.5,0.5,0.5],
    initial_radius=0.04,
    reduce_radius_by = 0.75,
    full_output=False
    ):

    ds = yt.load(enzo_dataset)
    sphere = ds.sphere(initial_guess,(initial_radius, "code_length"))

    threshold = sphere["index", "dx"].min().in_units("code_length").value

    iterations = 0
    
    eps = 1e9
    centers = [initial_guess]


    while (iterations < ITERATION_LIMIT and eps > threshold):

        radius = initial_radius*(reduce_radius_by**iterations)

        
        sphere = ds.sphere(centers[iterations],(radius, "code_length"))
        
        com = sphere.quantities.center_of_mass(use_particles=True)
        centers.append(
            np.array(com.to('code_length').value)
        )

        eps = np.linalg.norm(centers[iterations+1] - centers[iterations])

        # For debug purposes 
        # plot = yt.SlicePlot(
        #     ds, "z", ("gas", "density"),
        #     center=sphere.center,
        #     width=(2*radius, "code_length"),
        #     data_source=sphere
        # )

        # plot.save(f"iteration_{iterations}")

        iterations += 1

    # if the user wants to see the result of every iteration, e.g. for debug/convergence testing
    # return the result of every iteration
    if full_output:
        return centers
    # otherwise, just return the final result
    else:
        return centers[-1]