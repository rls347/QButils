import numpy as np
import h5py as hdf
import pkg_resources
import itertools
import QButils.files as QB
from rachelutils.hdfload import getvar
from QButils.plots import plotprofs

def gettupleprof(fil):
    prof = getvar(fil[1],'reflectivity')[:,0,0]
    return (fil[0],prof)

def getprof(fil):
    prof = getvar(fil,'reflectivity')[:,0,0]
    return (prof)

def getatten(fil):
    a=getvar(fil,'atten')[1,0,0]
    return a

def getmicroparams(runs):
    pvals = []
    svals = []
    avals = []
    gvals = []
    hvals = []
    for i in range(len(runs)):
        pvals.extend([item[2] for item in runs[i][1] if item[0] =='pristine'])
        svals.extend([item[2] for item in runs[i][1] if item[0] =='snow'])
        avals.extend([item[2] for item in runs[i][1] if item[0] =='aggregates'])
        gvals.extend([item[2] for item in runs[i][1] if item[0] =='graupel'])
        hvals.extend([item[2] for item in runs[i][1] if item[0] =='hail'])

    pvals=np.array(pvals)
    svals=np.array(svals)
    avals=np.array(avals)
    gvals=np.array(gvals)
    hvals=np.array(hvals)
    return pvals,svals,avals,gvals,hvals


def calcint(prof):
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    dh = np.diff(h)
    return np.sum(prof[1:]*dh)

def zspace(prof):
    newprof = 10**(prof/10.)
    newprof[prof<-900]=0
    return newprof

def maxval(prof):
    return np.max(prof)

def minval(prof):
    a=prof[prof>-998]
    return np.min(a)

def eth10(prof):
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    topz = 0
    eth = 0
    for z in range(len(h)-1,1,-1):
        if prof[z] > 10:
            topz = z
            break
    if topz > 0:
        sloperef = (prof[topz]-prof[topz+1])/(h[topz]-h[topz+1])
        eth = ((prof[topz]-10)/sloperef) + h[topz]
    return eth

def eth0(prof):
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    topz = 0
    eth = 0
    for z in range(len(h)-1,1,-1):
        if prof[z] > 0:
            topz = z
            break
    if topz > 0:
        sloperef = (prof[topz]-prof[topz+1])/(h[topz]-h[topz+1])
        eth = ((prof[topz]-0)/sloperef) + h[topz]
    return eth

def zintegral(prof):
    zprof = zspace(prof)
    integ = calcint(zprof)
    return integ

def zmean(prof):
    integ = zintegral(prof)
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    dh = np.diff(h)
    return (integ/np.sum(dh))

def profratio(p1,p2):
    return p1-p2

def maxprofratio(p1,p2):
    return np.max(p1-p2)

def intprofratio(p1,p2):
    ratio = profratio(p1,p2)
    return calcint(ratio)

def returnstats(var):
    type(var)
    v=np.array(var)
    return np.max(v), np.min(v), np.mean(v), np.std(v)

