import numpy as np
import h5py as hdf
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
    avals=np.array(svals)
    gvals=np.array(gvals)
    hvals=np.array(hvals)
    return pvals,svals,avals,gvals,hvals


def calcint(prof):
    h=getvar('QB1d.h5','z_coords')
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
    h=getvar('QB1d.h5','z_coords')
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
    h=getvar('QB1d.h5','z_coords')
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
    h=getvar('QB1d.h5','z_coords')
    dh = np.diff(h)
    return (integ/np.sum(dh))

def profdiff(fil1,fil2):
    p1 = getprof(fil1)
    p2 = getprof(fil2)
    z1 = zspace(p1)
    z2 = zspace(p2)
    return z1-z2

def profratio(fil1,fil2):
    p1 = getprof(fil1)
    p2 = getprof(fil2)
    z1 = zspace(p1)
    z2 = zspace(p2)
    ratio = z1/z2
    ratio[z2==0]=0.
    return ratio

def intprofdiff(fil1,fil2):
    diff = profdiff(fil1,fil2)
    return calcint(diff)

def intprofratio(fil1,fil2):
    ratio = profratio(fil1,fil2)
    return calcint(ratio)

def returnstats(var):
    type(var)
    v=np.array(var)
    return np.max(v), np.min(v), np.mean(v), np.std(v)


def runfiles(sc,nthreads):
    with open ('jan14runs.yml' ,'r') as sfile:
        runs = yaml.load(sfile)
    files = []
    freq = []
    pvals,svals,avals,gvals,hvals = getmicroparams(runs)
    pp = list(set(pvals))
    ss = list(set(svals))
    aa = list(set(avals))
    gg = list(set(gvals))
    hh = list(set(hvals))
    for x in runs:
        files.append(os.path.splitext(x[5])[0]+'.h5')
        freq.append(x[2]['freq'])
    freq = np.array(freq)
    tuplefiles = []
    ff = list(set(freq))


    for fr in ff:
        dbz10=[]
        dbz0=[]
        maxes=[]
        zint=[]
        meanz=[]

        for i,pris in enumerate(pp):
            nums = np.where(np.logical_and(freq==fr,pvals==pris))
            fil = [files[yy] for yy in nums[0]]
            tmpRDD = sc.parallelize(fil,nthreads).map(getprof)
            dbz10.append(tmpRDD.map(eth10).collect())
            dbz0.append(tmpRDD.map(eth0).collect())
            maxes.append(tmpRDD.map(maxval).collect())
            zint.append(tmpRDD.map(zintegral).collect())
            meanz.append(tmpRDD.map(zmean).collect())
        makebox(dbz10,str(fr)+'GHz 10dBZ ETH','Pristine Coef','box-eth10-'+str(fr)+'-pris.png')
        makebox(dbz0,str(fr)+'GHz 0dBZ ETH','Pristine Coef','box-eth0-'+str(fr)+'-pris.png')
        makebox(maxes,str(fr)+'GHz Max dBZ','Pristine Coef','box-maxes-'+str(fr)+'-pris.png')
        makebox(zint,str(fr)+'GHz Integrated Z','Pristine Coef','box-zint-'+str(fr)+'-pris.png')
        makebox(meanz,str(fr)+'GHz Mean Profile Z','Pristine Coef','box-meanz-'+str(fr)+'-pris.png')

        dbz10=[]
        dbz0=[]
        maxes=[]
        zint=[]
        meanz=[]

        for i,snow in enumerate(ss):
            nums = np.where(np.logical_and(freq==fr,svals==snow))
            fil = [files[yy] for yy in nums[0]]
            tmpRDD = sc.parallelize(fil,nthreads).map(getprof)
            dbz10.append(tmpRDD.map(eth10).collect())
            dbz0.append(tmpRDD.map(eth0).collect())
            maxes.append(tmpRDD.map(maxval).collect())
            zint.append(tmpRDD.map(zintegral).collect())
            meanz.append(tmpRDD.map(zmean).collect())
        makebox(dbz10,str(fr)+'GHz 10dBZ ETH','Snow Coef','box-eth10-'+str(fr)+'-snow.png')
        makebox(dbz0,str(fr)+'GHz 0dBZ ETH','Snow Coef','box-eth0-'+str(fr)+'-snow.png')
        makebox(maxes,str(fr)+'GHz Max dBZ','Snow Coef','box-maxes-'+str(fr)+'-snow.png')
        makebox(zint,str(fr)+'GHz Integrated Z','Snow Coef','box-zint-'+str(fr)+'-snow.png')
        makebox(meanz,str(fr)+'GHz Mean Profile Z','Snow Coef','box-meanz-'+str(fr)+'-snow.png')
 
        dbz10=[]
        dbz0=[]
        maxes=[]
        zint=[]
        meanz=[]

        for i,agg in enumerate(aa):
            nums = np.where(np.logical_and(freq==fr,avals==agg))
            fil = [files[yy] for yy in nums[0]]
            tmpRDD = sc.parallelize(fil,nthreads).map(getprof)
            dbz10.append(tmpRDD.map(eth10).collect())
            dbz0.append(tmpRDD.map(eth0).collect())
            maxes.append(tmpRDD.map(maxval).collect())
            zint.append(tmpRDD.map(zintegral).collect())
            meanz.append(tmpRDD.map(zmean).collect())
        makebox(dbz10,str(fr)+'GHz 10dBZ ETH','Agg Coef','box-eth10-'+str(fr)+'-agg.png')
        makebox(dbz0,str(fr)+'GHz 0dBZ ETH','Agg Coef','box-eth0-'+str(fr)+'-agg.png')
        makebox(maxes,str(fr)+'GHz Max dBZ','Agg Coef','box-maxes-'+str(fr)+'-agg.png')
        makebox(zint,str(fr)+'GHz Integrated Z','Agg Coef','box-zint-'+str(fr)+'-agg.png')
        makebox(meanz,str(fr)+'GHz Mean Profile Z','Agg Coef','box-meanz-'+str(fr)+'-agg.png')

        dbz10=[]
        dbz0=[]
        maxes=[]
        zint=[]
        meanz=[]

        for i,graup in enumerate(gg):
            nums = np.where(np.logical_and(freq==fr,gvals==graup))
            fil = [files[yy] for yy in nums[0]]
            tmpRDD = sc.parallelize(fil,nthreads).map(getprof)
            dbz10.append(tmpRDD.map(eth10).collect())
            dbz0.append(tmpRDD.map(eth0).collect())
            maxes.append(tmpRDD.map(maxval).collect())
            zint.append(tmpRDD.map(zintegral).collect())
            meanz.append(tmpRDD.map(zmean).collect())
        makebox(dbz10,str(fr)+'GHz 10dBZ ETH','Graupel Coef','box-eth10-'+str(fr)+'-graup.png')
        makebox(dbz0,str(fr)+ 'GHz 0dBZ ETH','Graupel Coef','box-eth0-'+str(fr)+'-graup.png')
        makebox(maxes,str(fr)+ 'GHz Max dBZ','Graupel Coef','box-maxes-'+str(fr)+'-graup.png')
        makebox(zint,str(fr)+ 'GHz Integrated Z','Graupel Coef','box-zint-'+str(fr)+'-graup.png')
        makebox(meanz,str(fr)+ 'GHz Mean Profile Z','Graupel Coef','box-meanz-'+str(fr)+'-graup.png')

        dbz10=[]
        dbz0=[]
        maxes=[]
        zint=[]
        meanz=[]

        for i,hail in enumerate(hh):
            nums = np.where(np.logical_and(freq==fr,hvals==hail))
            fil = [files[yy] for yy in nums[0]]
            tmpRDD = sc.parallelize(fil,nthreads).map(getprof)
            dbz10.append(tmpRDD.map(eth10).collect())
            dbz0.append(tmpRDD.map(eth0).collect())
            maxes.append(tmpRDD.map(maxval).collect())
            zint.append(tmpRDD.map(zintegral).collect())
            meanz.append(tmpRDD.map(zmean).collect())

        makebox(dbz10,str(fr)+'GHz 10dBZ ETH','Hail Coef','box-eth10-'+str(fr)+'-hail.png')
        makebox(dbz0,str(fr)+ 'GHz 0dBZ ETH','Hail Coef','box-eth0-'+str(fr)+'-hail.png')
        makebox(maxes,str(fr)+ 'GHz Max dBZ','Hail Coef','box-maxes-'+str(fr)+'-hail.png')
        makebox(zint,str(fr)+ 'GHz Integrated Z','Hail Coef','box-zint-'+str(fr)+'-hail.png')
        makebox(meanz,str(fr)+ 'GHz Mean Profile Z','Hail Coef','box-meanz-'+str(fr)+'-hail.png')
        


    for i in range(len(files)):
        tuplefiles.append((str(freq[i]),files[i]))

    ensembleRDD = sc.parallelize(files, nthreads)
#    maxes = ensembleRDD.map(maxval).collect()
#    dbz10 = ensembleRDD.map(eth10).collect()
#    dbz0 = ensembleRDD.map(eth0).collect()
#    zint = ensembleRDD.map(zintegral).collect()
#    meanz = ensembleRDD.map(zmean).collect()


    profiles = ensembleRDD.map(getprof).collect()
    plotprofs(profiles, 'allprofs.png')

    xRDD = sc.parallelize(tuplefiles,nthreads)
    profiles = xRDD.map(gettupleprof).groupByKey().collect()

    for i in range(3):
        plotprofs(profiles[i][1],'profiles-'+profiles[i][0]+'.png')
    profiles=None
    return profiles 




if __name__ == '__main__':
    starttime = time.time()
    nthreads=40
    sc = pysparkling.Context(pool=multiprocessing.Pool(nthreads),
                             serializer=cloudpickle.dumps,
                             deserializer=pickle.loads
                            )
    profs = runfiles(sc,nthreads)
    endtime = time.time()
    print endtime-starttime

    
