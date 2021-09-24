import yt, subprocess, os
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from mpi4py import MPI

IMG_RES = 1024

BOX_ORIGIN = ()

class FieldSetting:
    """
    Class that stores miscellaneous information associated with the fields
    we're querying like the colormap, plot title, units, and colorbar limits. 
    Exists for convenience purposes when plotting and collecting data.
    """

    def __init__(self, units, cmap, title, clim):
        self.units = units
        self.cmap = cmap
        self.title = title
        self.clim = clim

FIELD_SETTINGS = {
    'density' : FieldSetting(
        'g/cm**3',
        'viridis',
        r'Density (g/cm$^3$)',
        (1e-32, 1e-24)
    ),
    'metallicity' : FieldSetting(
        'Zsun',
        'dusk',
        r'Metallicity (Z/Z$_{odot}$)',
        (1e-8, 1)
    ),
    'temperature' : FieldSetting(
        'K',
        'plasma',
        r'Temperature (K)',
        (1e3, 1e7)
    ),
    ('deposit', 'stars_density') : FieldSetting(
        'g/cm**3',
        'Purples_r',
        r'Stellar Density (g/cm$^3$)',
        (1e-33, 1e-24)
    ),
}

FIELDS = ["density"]

def Query(dataset_fname, fields, box_origin, box_length):

    ds = yt.load(dataset_fname)

    x,y,z = box_origin
    dl = box_length
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    plot = yt.ProjectionPlot(ds, 'z', fields, 
            data_source=box, 
            weight_field='density', 
            buff_size=(IMG_RES, IMG_RES))

    # REMINDER: Change this when its time to do this with multiple fields
    for field in fields:
        units = FIELD_SETTINGS[field].units
        data = np.array(plot.frb[field].to(units).value)

    return data

def Analysis(args):
    pass