import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import make_axes_locatable
import yt
import numpy as np

plt.style.use("publication")

def projectionPlot(   data, 
            enzo_dataset=None, 
            field_name=None, 
            img_res=None, 
            box_length=None, 
            zlims=None, 
            cmap=None, 
            axes_units=None,
            output_file=None):


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
        
    # this is the only way to make a colorbar look good
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label(field_name)
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {output_file}")

    plt.title(f"z = {ds.current_redshift}")
    
    plt.savefig(output_file)
    plt.close()