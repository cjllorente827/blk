from IPython import display
import ipywidgets as widgets

def displayWidgets(self):

    for k, v in self.widget_list.items():
        v.visible and display.display(v)

def createWidget(self, name, type, 
    display_name=None, 
    default_value=None,
    disabled=False,
    visible=True):

    if display_name == None:
        display_name = name

    # populate the default value of our widget with Task arguments, if they exist
    if default_value == None:
        if name in self.arguments:
            default_value = self.arguments[name]

    # Determine if this widget should be disabled because it inherits its value
    # from a dependency
    if disabled == False and self.has_dependencies:
        if name in self.dependencies[0].arguments.keys():
            disabled = True

    if type == str:
        self.widget_list[name] = widgets.Text(
            value=default_value,
            description=display_name,
            layout={'width': 'initial'},
            style={'description_width': 'initial'},
            disabled=disabled
        )
        self.widget_list[name].visible = visible

    elif type == float:
        self.widget_list[name] = widgets.FloatText(
            value=default_value,
            description=display_name,
            layout={'width': 'initial'},
            style={'description_width': 'initial'}
        )
        self.widget_list[name].visible = visible


def setArgumentsFromWidgets(self):

    args = {}
    for name,widget in self.widget_list.items():
        if not hasattr(widget, "value"): continue
        args[name] = widget.value 

    self.setArguments(**args)

def createSaveAndRunButton(self, display_image=False):

    

    def saveAndRun(button):

        self.setArgumentsFromWidgets()
        print(str(self))

        result = self.getResult()

        if display_image:
            img = display.Image(result)
            display.display( img )
        else:
            display.display( result )
    # end saveAndRun

    self.widget_list["save_button"] = widgets.Button(
        description="Save and Run",
        tooltip="Saves settings and runs the Task",
        icon="play"
    )

    self.widget_list["save_button"].visible = True
    self.widget_list["save_button"].on_click(saveAndRun)
