
import yt
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
import numpy as np

plt.style.use("publication")
yt.set_log_level(40)

def refine_level_projection(  dataset, 
            projection_axis,
            box_origin, 
            box_length, 
            img_res, 
            field_units):

    ds = yt.load(dataset)

    x,y,z = box_origin
    dl = box_length*1.1 # include a small buffer around the box to avoid deadzones in the plot
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]
    field = ("index", "grid_level")

    proj = yt.OffAxisProjectionPlot(ds, projection_axis, field, 
        data_source=box,
        center=box_origin + 0.5 * box_length,
        buff_size=(img_res, img_res),
        method="mip",
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result

def refine_level_projection_plot(   data, 
            dataset, 
            img_res, 
            box_length, 
            zlims, 
            axes_units,
            plot_filename):


    ds = yt.load(dataset)
    comoving_box_size = ds.domain_width[0].to(axes_units)

    fig, ax = plt.subplots(figsize=(10,10))
    fig.subplots_adjust(
        left = 0.125,
        right = 0.9,
        bottom = 0.1,
        top = 0.9)

    # Fix the x and y axes
    L = box_length*comoving_box_size/2
    x = np.linspace(-L, L, img_res+1, endpoint=True)
    y = np.linspace(-L, L, img_res+1, endpoint=True)
    X,Y = np.meshgrid(x,y)
    
    im = ax.pcolormesh(X, Y, data["projection"], 
        cmap='jet', vmin=0, vmax=6)
        
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label("Refinement Level")
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {plot_filename}")
    
    plt.savefig(plot_filename)
    plt.close()
