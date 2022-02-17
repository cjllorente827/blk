
import yt
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1 import AxesGrid, make_axes_locatable
import numpy as np

plt.style.use("publication")
yt.set_log_level(40)

class HaloData:

    def __init__(self, id, Mvir, Rvir, X, Y, Z, catalog_fname):
        self.id = int(id)
        self.Mvir = Mvir
        self.Rvir = Rvir
        self.X = X
        self.Y = Y
        self.Z = Z
        self.catalog_fname = catalog_fname

    def __str__(self):
        return f"Halo {self.id}"



class Particle:

    def __init__(self, id, particle_type, mass, X, Y, Z):
        self.id = int(id)
        self.mass = mass
        self.particle_type = particle_type
        self.X = X
        self.Y = Y
        self.Z = Z

##############################################################################
# Returns all halos in the ascii catalog in a dictionary indexed by the 
# halo id.
##############################################################################
def get_halo_data_from_catalog(hc_fname, box_length):
    # Units: Masses in Msun / h
    # Units: Radii in kpc / h (comoving)
    # Units: Positions in Mpc / h (comoving)
    #
    # Distances will be returned in code units, i.e. 
    # floating point numbers from 0 to 1, where 1
    # represents the full length of the box

    ids, DescID, Mvir, Vmax, Vrms, Rvir, Rs, Np, X, Y, Z, *other = np.genfromtxt(hc_fname,
        skip_header=1,
        unpack=True)

    result = {}
    for i, halo_id in enumerate(ids):
        result[halo_id] = HaloData(
            ids[i], 
            Mvir[i], 
            Rvir[i]/1000/box_length, 
            X[i]/box_length, 
            Y[i]/box_length, 
            Z[i]/box_length,
            hc_fname)

    return result


def get_halo_from_catalog_by_id(hc_fname, target_id, box_length):
    # Units: Masses in Msun / h
    # Units: Radii in kpc / h (comoving)
    # Units: Positions in Mpc / h (comoving)
    #
    # Distances will be returned in code units, i.e. 
    # floating point numbers from 0 to 1, where 1
    # represents the full length of the box

    ids, DescID, Mvir, Vmax, Vrms, Rvir, Rs, Np, X, Y, Z, *other = np.genfromtxt(hc_fname,
        skip_header=1,
        unpack=True)

    result = None
    for i, halo_id in enumerate(ids):
        if halo_id == target_id:
            return HaloData(
                ids[i], 
                Mvir[i], 
                Rvir[i]/1000/box_length, 
                X[i]/box_length, 
                Y[i]/box_length, 
                Z[i]/box_length,
                hc_fname)

    print(f"No halo found with id {target_id}")
    return None

def get_halos_filtered_by(hc_fname, box_length, filter_func):
    # Units: Masses in Msun / h
    # Units: Radii in kpc / h (comoving)
    # Units: Positions in Mpc / h (comoving)
    #
    # Distances will be returned in code units, i.e. 
    # floating point numbers from 0 to 1, where 1
    # represents the full length of the box

    ids, DescID, Mvir, Vmax, Vrms, Rvir, Rs, Np, X, Y, Z, *other = np.genfromtxt(hc_fname,
        skip_header=1,
        unpack=True)

    result = {}
    for i, halo_id in enumerate(ids):
        new_halo = HaloData(
            ids[i], 
            Mvir[i], 
            Rvir[i]/1000/box_length, 
            X[i]/box_length, 
            Y[i]/box_length, 
            Z[i]/box_length,
            hc_fname)

        if filter_func(new_halo):
            result[halo_id] = new_halo

    return result


# Returns true if the halo is within 1 virial radius of a larger halo
# in the same catalog
def is_subhalo(all_halos, target_halo):
    
    for halo_id, halo in all_halos.items():
        if halo_id == target_halo.id:
            continue

        if halo.Mvir > target_halo.Mvir:
            
            r1 = np.array([
                target_halo.X,
                target_halo.Y,
                target_halo.Z
            ])

            r2 = np.array([
                halo.X,
                halo.Y,
                halo.Z
            ]) 

            distance = np.linalg.norm(r1-r2)
            

            if distance < halo.Rvir:
                return True

    # end for halo in all_halos
    return False



def calculate_extra_halo_quantities(dataset, halo):
    ds = yt.load(dataset)
    sp = ds.sphere((halo.X, halo.Y, halo.Z), halo.Rvir)
    bar = sp.argmax(("gas", "density"))
    com = sp.quantities.center_of_mass().to('code_length')
    dm = sp.argmax(('enzo', 'Dark_Matter_Density'))

    result = {}
    result["baryon_max_density"] = [float(x) for x in bar]
    result["dm_max_density"] = [float(x) for x in dm]
    result["center_of_mass"] = [float(x) for x in com]

    return result

def plot_halo(data, 
            dataset, 
            halo,
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
    L = box_length*comoving_box_size
    x = np.linspace(-L/2, L/2, img_res+1, endpoint=True)
    y = np.linspace(-L/2, L/2, img_res+1, endpoint=True)
    X,Y = np.meshgrid(x,y)
    
    im = ax.pcolormesh(X, Y, data["projection"], 
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

    rvir_annotation = patches.Circle((0., 0.),  
        halo.Rvir*comoving_box_size, 
        fill=False, 
        color="white",
        lw=1,
        transform=ax.transData)
    ax.add_patch(rvir_annotation)

    xtra = data["xtra"]
    com = xtra["center_of_mass"]
    dm = xtra["dm_max_density"]
    bar = xtra["baryon_max_density"]

    text_annotation = """COM ({0:2e},{1:2e},{2:2e})
DM ({3:2e},{4:2e},{5:2e})
bar ({6:2e},{7:2e},{8:2e})""".format(*com, *dm, *bar)

    bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    ax.text(-L/2, -1.33*L/2, text_annotation, ha="left", va="center", size=14,
            bbox=bbox_props)
    

    plot_title = f"Halo {halo.id}, " + \
        r"$M_{vir} = $ " + f"{halo.Mvir:.2e}" + \
        r" $M_{\odot}$"

    ax.set_title(plot_title)



    print(f"Saving to file: {plot_filename}")
    
    plt.savefig(plot_filename)
    plt.close()

def get_particles_in_halo(dataset, 
    halo_position,
    halo_radius):
    
    ds = yt.load(dataset)
    sp = ds.sphere(
        (halo_position[0],halo_position[1],halo_position[2]), 
        halo_radius
    )

    particle_type = np.array(sp[('all', 'particle_type')], dtype=int)
    dm_only = [t == 1 for t in particle_type]

    particle_data = {}
    particle_data["index"] = np.array(sp[('all', 'particle_index')], dtype=int)[dm_only]
    # particle_data["type"]    = np.array(sp[('all', 'particle_type')], dtype=int)[dm_only]
    # particle_data["mass"]  = np.array(sp[('all', 'particle_mass')].to('Msun'))[dm_only]
    particle_data["x"]  = np.array(sp[('all', 'particle_position_x')])[dm_only]
    particle_data["y"]  = np.array(sp[('all', 'particle_position_y')])[dm_only]
    particle_data["z"]  = np.array(sp[('all', 'particle_position_z')])[dm_only]
    
    return particle_data


def get_particles_by_id(data, dataset):
    
    ds = yt.load(dataset)
    ad = ds.all_data()

    for k,v in data.items():
        particles = v
    particle_ids = particles["index"]

    all_particles = np.array(ad[('all', 'particle_index')], dtype=int)
    array_mask = [np.where(all_particles == p)[0][0] for p in particle_ids]

    particle_data = {}
    particle_data["index"] = np.array(ad[('all', 'particle_index')], dtype=int)[array_mask]
    # particle_data["type"]    = np.array(ad[('all', 'particle_type')], dtype=int)[array_mask]
    # particle_data["mass"]  = np.array(ad[('all', 'particle_mass')].to('Msun'))[array_mask]
    particle_data["x"]  = np.array(ad[('all', 'particle_position_x')])[array_mask]
    particle_data["y"]  = np.array(ad[('all', 'particle_position_y')])[array_mask]
    particle_data["z"]  = np.array(ad[('all', 'particle_position_z')])[array_mask]

    return particle_data