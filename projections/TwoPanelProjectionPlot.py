import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import make_axes_locatable
import yt
import numpy as np

from blk.constants import DEFAULT_PLOT_SETTINGS
from blk.Tasks import Task

plt.style.use("publication")

class TwoPanelProjectionTask(Task):

    def __init__(self, 
        name="TwoPanelProjectionPlot", **kwargs):

        super().__init__(
            name=name,
            operation=twoPanelProjectionPlot,
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

        self.createSaveAndRunButton(display_image=True)
        self.displayWidgets()

def twoPanelProjectionPlot(   
    data, 
    data_keys=None,
    enzo_dataset=None, 
    fields=["density", "density"],
    xlabels=None,
    ylabels=None,
    zlabels=None, 
    img_res=1024, 
    length=None, 
    zlims=None, 
    cmap=None, 
    axes_units="Mpccm/h",
    output_file=None, **kwargs):

    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(20,10))

    if zlims == None:
        zlims = [
            DEFAULT_PLOT_SETTINGS[fields[0]]["zlims"],
            DEFAULT_PLOT_SETTINGS[fields[1]]["zlims"],
        ]
    if xlabels == None:
        xlabels = ['x']*2
    if ylabels == None:
        ylabels = ['y']*2
    if zlabels == None:
        zlabels = [
            DEFAULT_PLOT_SETTINGS[fields[0]]["zlabel"],
            DEFAULT_PLOT_SETTINGS[fields[1]]["zlabel"]
        ]
    if cmap == None:
        cmap = [
            DEFAULT_PLOT_SETTINGS[fields[0]]["cmap"],
            DEFAULT_PLOT_SETTINGS[fields[1]]["cmap"]
        ]

    ds = yt.load(enzo_dataset)
    comoving_box_size = ds.domain_width[0].to(axes_units)

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
    
    im1 = ax1.pcolormesh(X, Y, data[data_keys[0]], 
        cmap=cmap[0], 
        norm=LogNorm(vmin=zlims[0][0], vmax=zlims[0][1]))
        
    # this is the only way to make a colorbar look good
    divider = make_axes_locatable(ax1)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im1, cax=cax)
    cbar.set_label(zlabels[0])

    ax1.set_xlabel(f'{xlabels[0]}\n({axes_units})')
    ax1.set_ylabel(f'{ylabels[0]}\n({axes_units})')

    ax1.set_aspect('equal')

    im2 = ax2.pcolormesh(X, Y, data[data_keys[1]], 
        cmap=cmap[1], 
        norm=LogNorm(vmin=zlims[1][0], vmax=zlims[1][1]))
        
    # this is the only way to make a colorbar look good
    divider = make_axes_locatable(ax2)
    cax = divider.append_axes("right", size="5%", pad=0.05)
    cbar = fig.colorbar(im2, cax=cax)
    cbar.set_label(zlabels[1])
    
    ax2.set_xlabel(f'{xlabels[1]}\n({axes_units})')
    ax2.set_ylabel(f'{ylabels[1]}\n({axes_units})')  

    ax2.set_aspect('equal')

    print(f"Saving to file: {output_file}")

    plt.title(f"z = {ds.current_redshift}")
    
    plt.savefig(output_file)
    plt.close()