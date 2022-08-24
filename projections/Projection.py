
import yt
import numpy as np


yt.set_log_level(40)

AXES = {
    'x' : [1,0,0],
    'y' : [0,1,0],
    'z' : [0,0,1],
}

def projection(  
            enzo_dataset=None, 
            field=("gas", "density"), 
            projection_axis="x",
            box_center=[0.5,0.5,0.5], 
            box_length=1, 
            img_res=1024,
            use_mip=False,
            field_units="g/cm**3"):

    ds = yt.load(enzo_dataset)

    x,y,z = box_center
    half_length = box_length*1.1/2 # include a small buffer around the box to avoid deadzones in the plot
    box = ds.r[
        x-half_length:x+half_length, 
        y-half_length:y+half_length, 
        z-half_length:z+half_length, 
    ]

    method = "mip" if use_mip else "integrate"

    proj = yt.OffAxisProjectionPlot(ds, AXES[projection_axis], field, 
        weight_field=field, 
        data_source=box,
        north_vector=(0, 0, 1.),
        center=box_center,
        method=method,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result


