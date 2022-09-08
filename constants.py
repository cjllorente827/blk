

TASK_ACTION_OPTIONS = ["auto", "manual"]
AUTO, MANUAL = TASK_ACTION_OPTIONS

TASK_STATUS_OPTIONS = ["complete", "skipped", "failed"]
COMPLETE, SKIP, FAIL = TASK_STATUS_OPTIONS

DEPENDENCY_STRATEGIES = [
    "one-to-one", 
    "all-to-all", 
    "one-to-all", 
    "manual", 
    "none"
]

ONE_TO_ONE, ALL_TO_ALL, ONE_TO_ALL, MANUAL, NONE = DEPENDENCY_STRATEGIES

MAX_SEGMENTS = 10000
ITERATION_LIMIT = 10000

AXES = {
    'x' : [1,0,0],
    'y' : [0,1,0],
    'z' : [0,0,1],
}

NORTH_VECTORS = {
    'x' : [0,0,1],
    'y' : [1,0,0],
    'z' : [0,1,0],
}

ENZO_FIELDS = {
    "density" : ("gas", "density"),
    "temperature" : ("gas", "temperature"),
    "metallicity" : ("gas", "metallicity"),
    "grid_level" : ('index', "grid_level"),
    "tcool_tdyn_ratio" : ('gas', "tcool_tdyn_ratio"),
    "none" : None
}

DEFAULT_UNITS = {
    "density" : "g/cm**3",
    "temperature" : "K",
    "metallicity" : "Zsun",
    "grid_level" : "dimensionless",
    "tcool_tdyn_ratio" : "dimensionless"
}

DEFAULT_PLOT_SETTINGS = {
    "density" : {
        "zlabel" : r"Density\n(g cm$^{-3}$)",
        "zlims" : [1e-30, 5e-26],
        "field_units" : "g/cm**3",
        "cmap" : "viridis"
    },
    "temperature" : {
        "zlabel" : "Temperature (K)",
        "zlims" : [1e3, 1e8],
        "field_units" : "K",
        "cmap" : "plasma"
    },
    "metallicity" : {
        "zlabel" : "Metallicity\n(Zsun)",
        "zlims" : [1e-4, 1],
        "field_units" : "Zsun",
        "cmap" : "dusk"
    },
    "grid_level" : {
        "zlabel" : "Grid Level",
        "zlims" : [0, 6],
        "field_units" : "",
        "cmap" : "jet"
    },
    "tcool_tdyn_ratio" : {
        "zlabel" : r"$t_{cool} / t_{dyn}$",
        "zlims" : [1e-2, 1e2],
        "field_units" : "",
        "cmap" : "coolwarm"
    },
}