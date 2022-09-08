import yt
import numpy as np

def ytPhaseDiagram(
    enzo_dataset=None, 
    x_field=("gas", "density"), 
    y_field=("gas", "temperature"),
    z_field=[("gas", "mass")],
    weight_field=None,
    box_center=(0.5,0.5,0.5), 
    box_length=1,
    output_file=""
):
    ds = yt.load(enzo_dataset)

    # include a small buffer around the box to avoid deadzones in the plot
    dl = box_length*1.1 
    x,y,z = box_center - dl / 2  * np.ones(3)
    box = ds.r[x:x+dl, y:y+dl, z:z+dl]

    plot = yt.PhasePlot(
        box,
        x_field, 
        y_field, 
        z_field,
        weight_field=weight_field,
    )

    plot.set_cmap(z_field, "viridis")
    plot.save(output_file)