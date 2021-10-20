
import yt, subprocess, os, time
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
import numpy as np

plt.style.use("publication")

def projection(  dataset, 
            field, 
            projection_axis,
            box_origin, 
            box_length, 
            img_res, 
            field_units):

    ds = yt.load(dataset)

    x,y,z = box_origin
    dl = box_length*1.1 # include a small buffer around the box to avoid deadzones in the plot
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    proj = yt.OffAxisProjectionPlot(ds, projection_axis, field, 
        weight_field=field, 
        data_source=box,
        center=box_origin + 0.5 * box_length,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result

# A stage parallel version of the projection function
def projection_parallel(  dataset, 
            field, 
            projection_axis,
            box_origin, 
            box_length, 
            img_res, 
            field_units):

    yt.enable_parallelism()

    ds = yt.load(dataset)

    x,y,z = box_origin
    dl = box_length*1.1 # include a small buffer around the box to avoid deadzones in the plot
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    proj = yt.OffAxisProjectionPlot(ds, projection_axis, field, 
        weight_field=field, 
        data_source=box,
        center=box_origin + 0.5 * box_length,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result

def plot(   data, 
            dataset, 
            field, 
            img_res, 
            box_length, 
            field_units, 
            axes_units,
            plot_filename):


    ds = yt.load(dataset)
    comoving_box_size = ds.domain_width[0].to(axes_units)

    fig, ax = plt.subplots()

    # Fix the x and y axes
    L = box_length*comoving_box_size/2
    x = np.linspace(-L, L, img_res+1, endpoint=True)
    y = np.linspace(-L, L, img_res+1, endpoint=True)
    X,Y = np.meshgrid(x,y)
    
    im = ax.pcolormesh(X, Y, data["projection"], 
        cmap='viridis', 
        norm=LogNorm())
        
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label(field)
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {plot_filename}")
    plt.tight_layout()
    plt.savefig(plot_filename)
    plt.close()
