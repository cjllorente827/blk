
import yt

from yt.utilities.exceptions import YTFieldNotFound

def initializeExtraDerivedFields():

    yt.add_field(
        ("gas", "tcool_tdyn_ratio"),
        tcool_tdyn_ratio, 
        "cell"
    )


def tcool_tdyn_ratio(field, data):

    try :
        result = data["enzo", "Cooling_Time"] / data["gas", "dynamical_time"]

    except YTFieldNotFound as e:
        result = data["gas", "density"]*0

    return result

    



initializeExtraDerivedFields()