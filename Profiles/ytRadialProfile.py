
import yt
from blk.constants import DEFAULT_UNITS, ENZO_FIELDS

def ytRadialProfile(
    enzo_dataset=None,
    field="density",
    field_units=None,
    axes_units=None,
    center=[0.5,0.5,0.5],
    radius=0.1,
    output_file=None
):

    if field_units == None:
        field_units = DEFAULT_UNITS[field]
    if axes_units == None:
        axes_units = "kpc"

    ds = yt.load(enzo_dataset)
    my_sphere = ds.sphere(center, (radius, "code_length"))
    plot = yt.ProfilePlot(
        my_sphere, "radius", [ENZO_FIELDS[field]], weight_field=ENZO_FIELDS[field]
    )

    plot.set_unit(ENZO_FIELDS[field], field_units)
    plot.set_unit("radius", axes_units)
    plot.save(output_file)