
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

def projection(  dataset, 
            field, 
            box_center, 
            box_length, 
            img_res, 
            field_units,
            start_vector,
            rotation_axis,
            rotation_angle,
            use_mip):


    if rotation_axis not in Rotation_Matrix.keys():
        print(f"Invalid rotation axis: {rotation_axis}")
        return

    ds = yt.load(dataset)

    # include a small buffer around the box to avoid deadzones in the plot
    dl = box_length*1.1 
    x,y,z = box_center - dl / 2  * np.ones(3)
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    projection_axis = Rotation_Matrix[rotation_axis](rotation_angle) @ start_vector

    proj = yt.OffAxisProjectionPlot(ds, projection_axis, field, 
        weight_field=field, 
        data_source=box,
        north_vector=(0, 0, 1.),
        center=box_center,
        buff_size=(img_res, img_res),
        width=box_length)

    result = np.array(proj.frb[field].to(field_units).value)

    return result

def plot(   data, 
            dataset, 
            field_label, 
            img_res, 
            box_length, 
            zlims, 
            axes_units,
            cmap,
            plot_filename):


    ds = yt.load(dataset)
    comoving_box_size = ds.domain_width[0].to(axes_units)

    fig, ax = plt.subplots(figsize=(10,10))
    fig.subplots_adjust(
        left = 0.125,
        right = 0.9,
        bottom = 0.1,
        top = 0.9)

    # Apply correct units to the x and y axes
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
    cbar.set_label(field_label)
    
    ax.set_xlabel(f'x\n({axes_units})')
    ax.set_ylabel(f'y\n({axes_units})')  

    ax.set_aspect('equal')

    print(f"Saving to file: {plot_filename}")

    plt.title(f"z = {ds.current_redshift}")
    
    plt.savefig(plot_filename)
    plt.close()
