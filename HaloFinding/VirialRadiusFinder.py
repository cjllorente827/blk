
import numpy as np
import matplotlib.pyplot as plt
import yt

from blk import Cache, Task, SphereMeanDensity

def VirialRadiusFinder(
    enzo_dataset=None,
    center=[0.5,0.5,0.5],
    min_rad=1e-4,
    max_rad=1e-3, 
    overdensity_constant=200,
    cache_dir="./cache",
    make_test_plot=False
):

    yt.enable_parallelism()

    ds = yt.load(enzo_dataset)
    rho_crit = ds.cosmology.critical_density(ds.current_redshift)
    vir_threshold = overdensity_constant * rho_crit

    sphere = ds.sphere(center,(max_rad, "code_length"))

    dx = float(sphere["index", "dx"].min().in_units("code_length"))

    if make_test_plot:
        makeTestPlot(
            enzo_dataset,
            center,
            min_rad,
            max_rad,
            dx,
            rho_crit,
            overdensity_constant,
            cache_dir)

    test_rad = 0.5 * (max_rad + min_rad)

    delta_rad = 1e10 # large number

    iterations = 0
    MAX_ITERATIONS = 10000

    while (delta_rad > dx and iterations < MAX_ITERATIONS):

        density = SphereMeanDensity(
            ds,
            center,
            test_rad
        )

        if density > vir_threshold:
            min_rad = test_rad
        elif density < vir_threshold:
            max_rad = test_rad
        else:
            return test_rad

        new_test_rad = 0.5 * (max_rad + min_rad)

        delta_rad = abs(test_rad - new_test_rad)
        test_rad = new_test_rad
        iterations += 1

        iter_info = f"Iteration {iterations}: test_rad = {test_rad}, min_rad = {min_rad}, max_rad={max_rad}, delta_rad={delta_rad}"
        yt.is_root() and print(iter_info)

    rad_kpc = float((test_rad*ds.units.code_length).to('kpccm/h'))
    if yt.is_root():
        print(f"Virial radius found at {rad_kpc} kpccm/h")
        print(f"Virial threshold: {vir_threshold} g/cm^3")
        print(f"Mean density at {rad_kpc} kpccm/h : {density}")
    return test_rad # store the value in code units

def makeTestPlot(
    enzo_dataset,
    center,
    min_rad,
    max_rad,
    dx,
    rho_crit,
    overdensity_constant,
    cache_dir
):

    ds = yt.load(enzo_dataset)
    min_rad = max(dx, min_rad)

    r_values = np.logspace( np.log10(min_rad), np.log10(max_rad), 25) 

    vir_threshold = overdensity_constant * rho_crit

    mean_density = np.zeros_like(r_values)
    my_cache = Cache(cache_dir)
    for i,r in enumerate(r_values):
        
        next_task = Task(
            index=i,
            cache=my_cache,
            operation=SphereMeanDensity,
            arguments={
                "enzo_dataset": enzo_dataset,
                "center" : center,
                "radius" : r
            }
        )

        mean_density[i] = next_task.getResult()
    
    r_kpc = (r_values * ds.units.code_length).to('kpccm/h')
    plt.loglog(r_kpc, mean_density)
    plt.loglog(r_kpc, rho_crit*np.ones_like(r_values), c='k', linestyle="--", label=rf"$\rho_c$ at $z$ = {ds.current_redshift}")
    plt.loglog(r_kpc, vir_threshold*np.ones_like(r_values), c='r', linestyle="--", label=rf"{overdensity_constant}$\rho_c$")
    plt.xlabel("r (kpc)")
    plt.ylabel(r"Mean density of sphere with radius $r$ (g/cm$^3$)")
    plt.legend()
    plt.savefig("MeanDensityTest.png")