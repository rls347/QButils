import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection
from QButils.metrics import getvar
import pkg_resources

def plotprofs(profiles, filename, colorvar=None):
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    height = getvar(hfile,'z_coords')/1000.
    plotvars = []
    for var in profiles:
        plotvars.append((var,height))
    vararray = np.array(profiles)
    maxval = np.max(vararray)
    minval = np.min(vararray[vararray>-900])
    vararray[vararray<-900]=np.nan
    meanvar = np.nanmean(vararray,0)
    stdvar = np.nanstd(vararray,0)
    lines = [zip(x,y) for x, y in plotvars]
    fig, ax = plt.subplots()
    if colorvar is None:
        lines1 = LineCollection(lines, linewidth=2)
        ax.add_collection(lines1)
    else:
        lines1=LineCollection(lines,array = np.array(colorvar), cmap = plt.cm.plasma,linewidth=2)
        ax.add_collection(lines1)
#    plt.plot(meanvar,height,color='black',linewidth=2)
    #ax.set_ylim(0,18)
    #ax.set_xlim(-35,45)
    ax.set_ylim(0,height.max())
    ax.set_xlim(minval,maxval)
    plt.savefig(filename)
    plt.clf()
    plt.close()
    return

def makebox(var,titlename,xname,filename,hh):
    zipp = zip(hh,var)
    z = [x for _,x in sorted(zipp)]
    hh=sorted(hh)
    plt.boxplot(z,showfliers=False)
    plt.title(titlename)
    plt.xlabel(xname)
    plt.xticks(np.arange(len(hh))+1,hh)
    plt.savefig(filename)
    plt.clf()
    return

def meanplot(var, titlename, varname, filname, hh):
    yvar = np.zeros(len(var))
    xvar = np.array(hh)
    for i,v in enumerate(var):
        yvar[i] = np.mean(v)
    plt.scatter(xvar,yvar)
    plt.title(titlename)
    plt.xlabel(varname)
    plt.savefig(filname)
    plt.clf()
    plt.close()
    return
    
def plotdicts(dic,var):
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    height = getvar(hfile,'z_coords')/1000.
    for key in dic.keys():
        plt.plot(dic[key][var],height,label=key)
    plt.legend()
    plt.savefig('profile-'+var+'.png')
    plt.clf()
    plt.close()
    
    
    
    

