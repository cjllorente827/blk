from skimage import data
import napari

print (list(napari.utils.colormaps.AVAILABLE_COLORMAPS))

viewer = napari.view_image(data.moon(), colormap='viridis')
napari.run()  # start the event loop and show viewer

