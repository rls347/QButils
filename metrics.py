import numpy as np
import pkg_resources

def getvar(fil, varname):
    try:
        var = np.squeeze(fil[varname].value)
    except:
        filey = hdf.File(fil, 'r')
        var = np.squeeze(filey[varname].value)
        filey.close()
    return var

def gettupleprof(fil):
    '''When given a tuple of the form (x,filename), returns (x, reflectivity)'''
    prof = getvar(fil[1],'reflectivity')[:,0,0]
    return (fil[0],prof)

def getprof(fil):
    '''Gets a single reflectivity profile when given a filename.
        Will need to generalize to accept coordinates at some point. '''
    prof = getvar(fil,'reflectivity')[:,0,0]
    return (prof)

def getatten(fil):
    '''Gets a single path integrated attenuation value when given a filename.
        Will need to generalize to accept coordinates at some point. '''
    a=getvar(fil,'atten')[1,0,0]
    return a

def getmicroparams(runs):
    '''Gets micro parameters from original complicated yml file.
        I think I made this defunct by changing to dictionaries, h5. '''
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


def calcint(pf):
    '''Simple vertical integral. 
        Currently relies on QB1d.h5 existing in this directory.
        Will need to generalize later somehow.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    dh = np.diff(h)
    return np.sum(prof[1:]*dh)

def zspace(pf):
    '''Returns Z (mm^6/m^3) when given dBZ value.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    newprof = 10**(prof/10.)
    newprof[prof<-900]=0
    return newprof

def maxval(pf):
    '''Max reflectivity in profile.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    return np.max(prof)

def minval(pf):
    '''Max reflectivity in profile.
        Accounts for missing value from QB of -999.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    a=prof[prof>-998]
    return np.min(a)

def eth10(pf):
    '''Returns 10 dBZ echo top height. 
        Currently relies on QB1d.h5 existing in this directory.
        Will need to generalize later somehow.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
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

def eth0(pf):
    '''Returns 0 dBZ echo top height. 
        Currently relies on QB1d.h5 existing in this directory.
        Will need to generalize later somehow.
        Also should combine with eth10 maybe? Though much easier with 1 input.'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
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

def zintegral(pf):
    '''Returns the vertical integral of reflectivity (units - mm^6/m^2)'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    zprof = zspace(prof)
    integ = calcint(zprof)
    return integ

def zmean(pf):
    '''Returns the height mean value of reflectivity (units - mm^6/m^3)'''
    if type(pf) is str:
        prof = getprof(pf)
    else:
        prof = pf
    integ = zintegral(prof)
    hfile = pkg_resources.resource_filename(__name__, 'QB1d.h5')
    h = getvar(hfile,'z_coords')
    dh = np.diff(h)
    return (integ/np.sum(dh))

def profratio(pf1,pf2):
    '''Dual Wavelength Ratio - difference between 2 frequencies in dBZ space'''
    if type(pf1) is str:
        p1 = getprof(pf1)
    else:
        p1 = pf1
    if type(pf2) is str:
        p2 = getprof(pf2)
    else:
        p2 = pf2   
    return p1-p2

def maxprofratio(p1,p2):
    '''Max value in profile of DWR (dBZ)'''
    prat = profratio(p1,p2)
    return np.max(prat)

def intprofratio(p1,p2):
    '''Vertical integral of DWR...
        Currently all done in dBZ space but need to think on this'''
    ratio = profratio(p1,p2)
    return calcint(ratio)

def returnstats(var):
    type(var)
    v=np.array(var)
    return np.max(v), np.min(v), np.mean(v), np.std(v)

