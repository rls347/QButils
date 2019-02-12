import numpy as np

def refvshydro(ref,hydro):
    hydrobins= np.linspace(0.0001,np.max(hydro),40)
    radarbins = np.arange(-30,40,2)

    x=ref.flatten()
    y=hydro.flatten()


    H, xedges, yedges = np.histogram2d(y,x, bins=[hydrobins, radarbins])
    for z in range(H.shape[0]):
        if (np.sum(H[z,:]) != 0): H[z,:] = H[z,:]/np.sum(H[z,:])
    H[np.isnan(H)] = 0

    refmean = np.zeros(len(hydrobins)-1)
    gmean = np.zeros(len(hydrobins)-1)
    refstd= np.zeros(len(hydrobins)-1)

    for i in range(len(gmean)):
        gmean[i] = (hydrobins[i]+hydrobins[i+1])/2.
        a=np.logical_and(hydro>hydrobins[i], hydro<hydrobins[i+1])
        refmean[i] = np.mean(ref[a])
        refstd[i] = np.std(ref[a])

    return gmean, refmean,refstd
