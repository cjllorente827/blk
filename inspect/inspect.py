import yt
import numpy as np
import matplotlib.pyplot as plt

def inspect_slices(
    dataset, 
    center, 
    width, 
    field, 
    units,
    resolution=128):

    ds = yt.load(dataset)

    num_samples = resolution

    dx = np.ones(3) * width/2
    x0, y0, z0 = center - dx
    x1, y1, z1 = center + dx

    z_samples = np.linspace(z0, z1, num_samples)
    
    result = np.zeros((num_samples, resolution, resolution))

    for i,z in enumerate(z_samples):

        slc = ds.r[x0:x1,y0:y1,z]

        frb = yt.FixedResolutionBuffer(slc, (x0, x1, y0, y1), (resolution, resolution))

        result[i] = np.array(frb[field].to(units).value)

        # plt.pcolormesh(result[i])
        # plt.savefig(f"density{i}.png")
        # print(f"Saved density{i}.png")
    #end for i,z

    return result

