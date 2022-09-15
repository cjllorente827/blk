
import yt
import numpy as np
from yt.frontends.enzo.data_structures import EnzoDataset

# TODO: Figure out something to handle the units better
def SphereMeanDensity(
    enzo_dataset,
    center=[0.5,0.5,0.5], 
    radius=0.01 # must be code length
):

    dataset_arg_type = type(enzo_dataset)
    if dataset_arg_type == str:
        ds = yt.load(enzo_dataset)
    elif dataset_arg_type == EnzoDataset:
        ds = enzo_dataset
    else:
        print(f"[Error] enzo_dataset has invalid type: {dataset_arg_type}")
        exit()

    sphere = ds.sphere(center, radius)

    total_mass = float(sphere.quantities.total_mass().sum().to("g").value)

    r_cm = float( (radius * ds.units.code_length).to('cm') )

    vol = 4/3 * np.pi * r_cm**3

    return total_mass / vol