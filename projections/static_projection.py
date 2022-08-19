
import yt
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
import numpy as np

plt.style.use("publication")
yt.set_log_level(40)

AXES = {
    'x' : [1,0,0],
    'y' : [0,1,0],
    'z' : [0,0,1],
}

def static_projection(  
            enzo_dataset=None, 
            field=None, 
            projection_axis=None,
            box_center=None, 
            box_length=1, 
            img_res=1024, 
            field_units=None):

    ds = yt.load(enzo_dataset)

    x,y,z = box_center
    half_length = box_length*1.1/2 # include a small buffer around the box to avoid deadzones in the plot
    box = ds.r[
        x-half_length:x+half_length, 
        y-half_length:y+half_length, 
        z-half_length:z+half_length, 
    ]

    proj = yt.OffAxisProjectionPlot(ds, AXES[projection_axis], field, 
        weight_field=field, 
        data_source=box,
        center=box_center,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result

def static_projection_plot(   data, 
            enzo_dataset=None, 
            field_name=None, 
            img_res=None, 
            box_length=None, 
            zlims=None, 
            cmap=None, 
            axes_units=None,
            output_file=None):


    print(data)
    ds = yt.load(enzo_dataset)
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
    
    im = ax.pcolormesh(X, Y, data, 
        cmap=cmap, 
        norm=LogNorm(vmin=zlims[0], vmax=zlims[1]))
        
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label(field_name)
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {output_file}")
    
    plt.savefig(output_file)
    plt.close()
