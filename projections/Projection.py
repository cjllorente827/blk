

import yt, os, re
import numpy as np

from blk.Tasks import Task
from blk.constants import AXES, DEFAULT_UNITS, ENZO_FIELDS, NORTH_VECTORS
from blk.projections import ExtraDerivedFields
from blk.constants import ENZO_FIELDS

yt.set_log_level(40)

import ipywidgets as widgets
from ipywidgets import GridspecLayout
from IPython.display import display

class ProjectionTask(Task):

    def __init__(self, 
        name="projection", 
        cache=None,
        pipeline=None,
        arguments=None,
        index=None,
        dependencies=None):

        self.cache = None

        super().__init__(
            name=name,
            pipeline=pipeline,
            cache=cache,
            operation=projection,
            arguments=arguments,
            index=index,
            dependencies=dependencies,
            save_action="auto",
            always_run=False
        )
        
    # end __init__

    def UI(self):

        self.widget_list = {}
        self.widget_list["enzo_out_dir"] = widgets.Text(
            value='/mnt/gs21/scratch/llorente/halo_4954_lowres/fiducial_run',
            placeholder='Path to Enzo output',
            description='Enzo data directory:',
            disabled=False,
            layout={'width': 'initial'},
            style={'description_width': 'initial'}
        )

        enzo_out_dir = self.widget_list["enzo_out_dir"].value
        file_pattern = re.compile("RD[0-9][0-9][0-9][0-9]")
        files = [os.path.join(f,f) for f in os.listdir(enzo_out_dir) if file_pattern.match(f) ]
        files.sort()

        self.widget_list["enzo_dataset"] = widgets.Dropdown(
            options=files,
            value=files[111],
            description='Enzo dataset:',
            disabled=False,
        )

        default_fields = ENZO_FIELDS.keys()
        self.widget_list["field"] = widgets.Dropdown(
            options=default_fields,
            value="density",
            description='Projection field: ',
            disabled=False,
        )

        self.widget_list["cube_length"] = widgets.BoundedFloatText(
            value=0.08,
            min=0,
            max=1.0,
            step=0.001,
            description='Box Length:',
            disabled=True
        )

        self.widget_list["sphere_rad"] = widgets.BoundedFloatText(
            value=0.08,
            min=0,
            max=1.0,
            step=0.001,
            description='Sphere Radius:',
            disabled=True
        )

        self.widget_list["shape"] = widgets.Dropdown(
            options=["sphere", "cube"],
            value="cube",
            description='Shape of region: ',
            disabled=False,
        )


        center_grid = GridspecLayout(3, 1)
        center_grid.disabled = False

        default_xyz = [0.486, 0.502, 0.508]

        for i, coord in enumerate(['x', 'y', 'z']):
            center_grid[i,0] = widgets.BoundedFloatText(
                value=default_xyz[i],
                min=0,
                max=1.0,
                step=0.001,
                description=f'{coord}:',
                disabled=False
            )
        # end for coord

        self.widget_list["center_grid"] = center_grid

        if self.widget_list["shape"].value == "cube":
            self.widget_list["cube_length"].disabled = False
        elif self.widget_list["shape"].value == "sphere":
            self.widget_list["sphere_rad"].disabled = False

        self.widget_list["save_button"] = widgets.Button(
            description="Save and Run",
            disabled = False,
            tooltip="Saves settings and runs the Task",
            icon="play"
        )

        def save_and_run(button):

            enzo_dataset = os.path.join(self.widget_list["enzo_out_dir"].value, self.widget_list["enzo_dataset"].value)
            center_grid = self.widget_list["center_grid"]

            center = [
                center_grid[0, 0].value, 
                center_grid[1, 0].value, 
                center_grid[2, 0].value
            ]

            arguments = {
                "enzo_dataset" : enzo_dataset,
                "field" : self.widget_list["field"].value,
                "center" : center,
                "shape": self.widget_list["shape"].value
            }

            if self.widget_list["shape"].value == "cube":
                arguments["length"] = self.widget_list["cube_length"].value
            elif self.widget_list["shape"].value == "sphere":
                arguments["length"] = self.widget_list["sphere_rad"].value

            self.setArguments(**arguments)

            print(str(self))
            display(self.getResult())

            

        self.widget_list["save_button"].on_click(save_and_run)

        for k, v in self.widget_list.items():
            
            not v.disabled and display(v)


def projection(  
            enzo_dataset=None, 
            field="density", 
            weight_field="density",
            projection_axis="z",
            center=[0.5,0.5,0.5], 
            length=1, 
            shape="cube",
            img_res=1024,
            use_mip=False,
            field_units=None):

    if field_units == None:
        field_units = DEFAULT_UNITS[field]

    ds = yt.load(enzo_dataset)

    if shape == "cube" or "box":
        x,y,z = center
        half_length = length*1.05/2 # include a small buffer around the box to avoid deadzones in the plot
        data_source = ds.r[
            x-half_length:x+half_length, 
            y-half_length:y+half_length, 
            z-half_length:z+half_length, 
        ]
        width = length
    elif shape == "sphere" or "ball":
        data_source = ds.sphere(center, length)
        width = 2*length


    if use_mip:
        method = "mip" 
        weight_field = None
    else:
        method = "integrate"
        weight_field = ENZO_FIELDS[weight_field]

    proj = yt.ProjectionPlot(ds, 
        projection_axis, 
        ENZO_FIELDS[field], 
        weight_field=weight_field, 
        data_source=data_source,
        center=center,
        method=method,
        buff_size=(img_res, img_res),
        width=width)

    result = np.array(proj.frb[field].to(field_units).value)

    return result


