fname = '/mnt/research/galaxies-REU/sims/cosmological/set1_LR/halo_008508/RD0042/RD0042'
#out_fname = 'two_by_two_test_z_0.mp4'


####################################################################
# 25 Mpc outputs
####################################################################
#fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0265/RD0265"

# fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0166/RD0166"

# fname = "/mnt/home/llorente/cosmo_bigbox/25Mpc_512/RD0111/RD0111"



####################################################################
# 50 Mpc outputs
####################################################################

# fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0061/RD0061"

# fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0088/RD0088"

# fname = "/mnt/home/llorente/cosmo_bigbox/50Mpc_512/RD0135/RD0135"


import yt, sys
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

yt.funcs.mylog.setLevel(50)

ds = yt.load(fname)

ad = ds.all_data()

plot = yt.PhasePlot(ad, ("gas", "density"), 
                        ("gas", "temperature"), 
                        [("gas", "mass")], 
                        weight_field=None)

plot.set_cmap(("gas", "mass"), 'plasma')
plot.save()






