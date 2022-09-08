

import yt
import numpy as np

from blk.constants import AXES, DEFAULT_UNITS, ENZO_FIELDS, NORTH_VECTORS

from blk.projections import ExtraDerivedFields

yt.set_log_level(40)



def projection(  
            enzo_dataset=None, 
            field="density", 
            weight_field="density",
            projection_axis="z",
            center=[0.5,0.5,0.5], 
            length=1, 
            shape="cube",
            img_res=1024,
            use_mip=False,
            field_units=None):

    if field_units == None:
        field_units = DEFAULT_UNITS[field]

    ds = yt.load(enzo_dataset)

    if shape == "cube" or "box":
        x,y,z = center
        half_length = length*1.05/2 # include a small buffer around the box to avoid deadzones in the plot
        data_source = ds.r[
            x-half_length:x+half_length, 
            y-half_length:y+half_length, 
            z-half_length:z+half_length, 
        ]
        width = length
    elif shape == "sphere" or "ball":
        data_source = ds.sphere(center, length)
        width = 2*length


    if use_mip:
        method = "mip" 
        weight_field = None
    else:
        method = "integrate"
        weight_field = ENZO_FIELDS[weight_field]

    proj = yt.ProjectionPlot(ds, 
        projection_axis, 
        ENZO_FIELDS[field], 
        weight_field=weight_field, 
        data_source=data_source,
        center=center,
        method=method,
        buff_size=(img_res, img_res),
        width=width)

    result = np.array(proj.frb[field].to(field_units).value)

    return result


