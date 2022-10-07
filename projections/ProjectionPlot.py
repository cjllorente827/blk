import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import make_axes_locatable
import yt, os, re
import numpy as np
import ipywidgets as widgets
from IPython import display

plt.style.use("publication")
#plt.style.use("notebook")

from blk.constants import DEFAULT_PLOT_SETTINGS, ENZO_FIELDS
from blk.Tasks import Task

class ProjectionPlotTask(Task):

    def __init__(self, 
        name="projectionPlot", **kwargs):

        super().__init__(
            name=name,
            operation=projectionPlot,
            save_action="manual",
            always_run=True,
            **kwargs
        )
        
    # end __init__

    def UI(self):

        self.createWidget(
            "enzo_dataset", 
            str, 
            display_name="Enzo Dataset:"
        )

        self.createWidget(
            "output_file", 
            str, 
            display_name="Image filename:"
        )

        self.createWidget(
            "cmap", 
            str, 
            display_name="Colormap:", 
            default_value="viridis"
        )

        self.createSaveAndRunButton(display_image=True)
        self.displayWidgets()

def projectionPlot(   data, 
            enzo_dataset=None, 
            field="density",
            zlabel=None, 
            img_res=1024, 
            length=None, 
            zlims=None, 
            cmap=None, 
            axes_units="Mpccm/h",
            output_file=None,
            use_log=True,
            **kwargs):

    if zlims == None:
        zlims = DEFAULT_PLOT_SETTINGS[field]["zlims"]
    if zlabel == None:
        zlabel = DEFAULT_PLOT_SETTINGS[field]["zlabel"]
    if cmap == None:
        cmap = DEFAULT_PLOT_SETTINGS[field]["cmap"]
    


    ds = yt.load(enzo_dataset)
    comoving_box_size = ds.domain_width[0].to(axes_units)

    #fig, (ax1, ax2) = plt.subplots(1, 2)

    fig, ax = plt.subplots(figsize=(10,10))
    fig.subplots_adjust(
        left = 0.125,
        right = 0.9,
        bottom = 0.1,
        top = 0.9)

    # Fix the x and y axes
    L = length*comoving_box_size/2
    x = np.linspace(-L, L, img_res+1, endpoint=True)
    y = np.linspace(-L, L, img_res+1, endpoint=True)
    X,Y = np.meshgrid(x,y)
    
    if use_log:
        im = ax.pcolormesh(X, Y, data, 
            cmap=cmap, 
            norm=LogNorm(vmin=zlims[0], vmax=zlims[1]))
    else:
        im = ax.pcolormesh(X, Y, data, cmap=cmap, vmin=zlims[0], vmax=zlims[1])
        
    # this is the only way to make a colorbar look good
    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im, cax=cax)
    cbar.set_label(zlabel)
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {output_file}")

    plt.title(f"z = {ds.current_redshift}")
    
    plt.savefig(output_file)
    plt.close()