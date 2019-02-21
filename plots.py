import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection
from rachelutils.hdfload import getvar

def plotprofs(profiles, filename, colorvar=None):
    height = getvar('QB1d.h5','z_coords')/1000.
    plotvars = []
    for var in profiles:
        plotvars.append((var,height))
    vararray = np.array(profiles)
    vararray[vararray<-900]=np.nan
    meanvar = np.nanmean(vararray,0)
    stdvar = np.nanstd(vararray,0)
    lines = [zip(x,y) for x, y in plotvars]
    fig, ax = plt.subplots()
    if colorvar is None:
        lines1 = LineCollection(lines, linewidth=1)
        ax.add_collection(lines1)
    else:
        lines1=LineCollection(lines,array = colorvar, cmap = plt.cm.plasma,linewidth=2)
        ax.add_collection(lines1)
    plt.plot(meanvar,height,color='black',linewidth=2)
    ax.set_ylim(0,18)
    ax.set_xlim(-35,45)
    plt.savefig(filename)
    plt.clf()

    return

