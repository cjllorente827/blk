
import yt
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
import numpy as np

plt.style.use("publication")
yt.set_log_level(40)

Rx = lambda tht: np.array([
        [1, 0, 0],
        [0, np.cos(tht), -np.sin(tht)],
        [0, np.sin(tht), np.cos(tht)]
    ])

Ry = lambda tht: np.array([
        [np.cos(tht), 0, np.sin(tht)],
        [0, 1, 0],
        [-np.sin(tht), 0, np.cos(tht)]
    ])

Rz = lambda tht: np.array([
        [np.cos(tht), -np.sin(tht), 0],
        [np.sin(tht), np.cos(tht), 0],
        [0, 0, 1]
    ])

Rotation_Matrix = {
    "x": Rx,
    "y": Ry,
    "z": Rz
}

def rotatingProjection(  
            enzo_dataset=None, 
            field=("gas", "density"), 
            box_center=(0.5,0.5,0.5), 
            box_length=1, 
            img_res=1024, 
            field_units="g/cm**3",
            start_vector=(1,0,0),
            rotation_axis="z",
            rotation_angle=0,
            use_mip=False):


    if rotation_axis not in Rotation_Matrix.keys():
        print(f"Invalid rotation axis: {rotation_axis}")
        return

    ds = yt.load(enzo_dataset)

    # include a small buffer around the box to avoid deadzones in the plot
    dl = box_length*1.1 
    x,y,z = box_center - dl / 2  * np.ones(3)
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    projection_axis = Rotation_Matrix[rotation_axis](rotation_angle) @ start_vector

    method = "mip" if use_mip else "integrate"

    proj = yt.OffAxisProjectionPlot(ds, projection_axis, field, 
        weight_field=field, 
        data_source=box,
        north_vector=(0, 0, 1.),
        center=box_center,
        method=method,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result


