
import yt
import numpy as np

from blk.constants import ENZO_FIELDS, DEFAULT_UNITS

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
            field="density", 
            center=[0.5,0.5,0.5], 
            length=1, 
            shape="cube",
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

    if shape == "cube" or "box":
        x,y,z = center
        half_length = length*1.05/2 # include a small buffer around the box to avoid deadzones in the plot
        data_source = ds.r[
            x-half_length:x+half_length, 
            y-half_length:y+half_length, 
            z-half_length:z+half_length, 
        ]
    elif shape == "sphere" or "ball":
        data_source = ds.sphere(center, length)

    projection_axis = Rotation_Matrix[rotation_axis](rotation_angle) @ start_vector

    method = "mip" if use_mip else "integrate"

    proj = yt.OffAxisProjectionPlot(ds, 
        projection_axis, 
        ENZO_FIELDS[field], 
        weight_field=("gas", "density"), 
        data_source=data_source,
        north_vector=(0, 1., 0),
        center=center,
        method=method,
        buff_size=(img_res, img_res),
        width=2*length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result


def calculateRotationAngle(index=0, num_tasks=1):

    return index/num_tasks * 2 * np.pi