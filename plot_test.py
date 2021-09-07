import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import blk

plt.style.use("publication")

track_hash = 'a6429336f731eb8a6f2ded016325f485'
track, time = blk.load_result_from_cache(track_hash)

plt.pcolormesh( 10**track[0], 10**track[1], track[2],
            norm=mcolors.LogNorm(), cmap="plasma")
ax = plt.gca()

ax.set(
    xlabel=r'Density $\left( \frac{\rm{g}}{\rm{cm}^3} \right)$',
    ylabel=r"Temperature ($\rm{K}$)",
    xscale='log',
    yscale='log')
cbar = plt.colorbar(pad=0)
cbar.set_label(r"Total Mass ($\rm{M_{\odot}}$)")
cbar.ax.tick_params(direction = 'out')

plt.title(r"25 Mpc box at $z = 0$")

#plt.savefig('phase')
plt.show()
plt.close()

