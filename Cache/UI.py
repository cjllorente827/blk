from faulthandler import disable
import ipywidgets as widgets
from IPython.display import display

def UI(self):
    self.w_cache = widgets.Text(
        value='/mnt/gs21/scratch/llorente/halo_4954_lowres/fiducial_run/cache',
        placeholder='Path to blk cache',
        description='blk cache:',
        disabled=False,
        layout={'width': 'initial'},
        style={'description_width': 'initial'}
    )

    self.save_button = widgets.Button(
        description="Create cache",
        disabled = False,
        tooltip="Sets the current value of the text box to the cache directory",
        icon="check"
    )

    def save_setting(button):
        self.setDirectory(self.w_cache.value)

    self.save_button.on_click(save_setting)

    display(self.w_cache)
    display(self.save_button)