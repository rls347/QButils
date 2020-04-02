import numpy as np
import h5py as hdf

def getvar(fil, varname):
    try:
        var = np.squeeze(fil[varname].value)
    except:
        filey = hdf.File(fil, 'r')
        var = np.squeeze(filey[varname].value)
        filey.close()
    return var

def makesingleprofile(ramsfile,profilefile,xval,yval):
    vars3d = ['agg_concen_kg','aggregates','cloud','cloud_concen_mg','drizzle',
            'drizzle_concen_mg','geo','graup_concen_kg','graupel','hail','hail_concen_kg',
            'press','pris_concen_kg','pristine','rain','rain_concen_kg','relhum',
            'snow','snow_concen_kg','tempk','theta','theta_e','total_cond','u','ue','v','vapor',
            've','w']

    vars2d = ['albedt','land','lat','lon','pcpg','pcprate','speed10m','sst','tempf2m','topt','totpcp','vertint_cond','vertint_vapor']

    try:
        ff=hdf.File(ramsfile,'r')

        with hdf.File(profilefile,'w') as hf:
            for var in vars3d:
                tmp = ff[var].value[:,:,yval:yval+1,xval:xval+1]
                varout = np.transpose(tmp,(3,2,1,0)) 
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            for var in vars2d:
                tmp = ff[var].value[:,yval:yval+1,xval:xval+1]
                varout = np.transpose(tmp,(2,1,0))
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            a=hf.create_dataset('t_coords',data=ff['t_coords'].value)
            for tt in ff['t_coords'].attrs.keys():
                a.attrs[tt]=ff['t_coords'].attrs[tt]
            a=hf.create_dataset('z_coords',data=ff['z_coords'].value)
            for tt in ff['z_coords'].attrs.keys():
                a.attrs[tt]=ff['z_coords'].attrs[tt]
            a=hf.create_dataset('x_coords',data=ff['x_coords'].value[xval:xval+1])
            for tt in ff['x_coords'].attrs.keys():
                a.attrs[tt]=ff['x_coords'].attrs[tt]
            a=hf.create_dataset('y_coords',data=ff['y_coords'].value[yval:yval+1])
            for tt in ff['y_coords'].attrs.keys():
                a.attrs[tt]=ff['y_coords'].attrs[tt]
    except:
        profilefile = None
        
    return profilefile


def perturbprofile(infile,outfile,perturbations,flag=None):
    vars = ['agg_concen_kg','aggregates','cloud','cloud_concen_mg','drizzle',
            'drizzle_concen_mg','graup_concen_kg','graupel','hail','hail_concen_kg',
            'press','pris_concen_kg','pristine','rain','rain_concen_kg','relhum',
            'snow','snow_concen_kg','tempk','vapor','w']

    vars2d = ['lat','lon','pcprate']

    try:
        ff=hdf.File(infile,'r')

        with hdf.File(outfile,'w') as hf:
            for var in vars:
                varout = ff[var].value[:]
                if var in perturbations:
                    if flag == 'factor' and type(perturbations[var])==float:
                        varout = varout * (1+perturbations[var])
                    elif flag == 'mult':
                        tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        varout = varout * tmp
                    else:
                        if type(perturbations[var])==np.ndarray:
                            tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        else:
                            tmp=perturbations[var]
                        varout = varout + tmp
                        # if Y is not None:
#                             Y=Y.reshape(varout.shape)
#                             zer = np.where(np.logical_or(varout < 0,Y < -900))
#                         else:
#
                        zer = np.where(varout<0)
                        #varout[zer]=0.0
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            for var in vars2d:
                varout = ff[var].value[:]
                if var in perturbations:
                    if flag == 'factor':
                        varout = varout * (1+perturbations[var])
                    elif flag == 'mult':
                        tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        varout = varout * tmp
                    else:
                        if type(perturbations[var])==np.ndarray:
                            tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        else:
                            tmp=perturbations[var]
                        varout = varout + tmp
                        #varout[varout<0]=0.0
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            a=hf.create_dataset('t_coords',data=ff['t_coords'].value)
            for tt in ff['t_coords'].attrs.keys():
                a.attrs[tt]=ff['t_coords'].attrs[tt]
            a=hf.create_dataset('z_coords',data=ff['z_coords'].value)
            for tt in ff['z_coords'].attrs.keys():
                a.attrs[tt]=ff['z_coords'].attrs[tt]
            a=hf.create_dataset('x_coords',data=ff['x_coords'].value)
            for tt in ff['x_coords'].attrs.keys():
                a.attrs[tt]=ff['x_coords'].attrs[tt]
            a=hf.create_dataset('y_coords',data=ff['y_coords'].value)
            for tt in ff['y_coords'].attrs.keys():
                a.attrs[tt]=ff['y_coords'].attrs[tt]
    except:
        outfile = None
    return outfile



def changevalue(infile,outfile,perturbations):
    vars = ['agg_concen_kg','aggregates','cloud','cloud_concen_mg','drizzle',
            'drizzle_concen_mg','graup_concen_kg','graupel','hail','hail_concen_kg',
            'press','pris_concen_kg','pristine','rain','rain_concen_kg','relhum',
            'snow','snow_concen_kg','tempk','vapor','w']

    vars2d = ['lat','lon','pcprate']

    try:
        ff=hdf.File(infile,'r')

        with hdf.File(outfile,'w') as hf:
            for var in vars:
                varout = ff[var].value[:]
                if var in perturbations:
                    if type(perturbations[var])==np.ndarray:
                        tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        varout = tmp
                    else:
                        tmp=perturbations[var]
                        varout[:] = tmp
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            for var in vars2d:
                varout = ff[var].value[:]
                if var in perturbations:
                    if type(perturbations[var])==np.ndarray:
                        tmp=perturbations[var].reshape(varout.shape).astype(np.float32)
                        varout = tmp
                    else:
                        tmp=perturbations[var]
                        varout[:] = tmp
                a=hf.create_dataset(var,data=varout)
                for tt in ff[var].attrs.keys():
                    a.attrs[tt]=ff[var].attrs[tt]
            a=hf.create_dataset('t_coords',data=ff['t_coords'].value)
            for tt in ff['t_coords'].attrs.keys():
                a.attrs[tt]=ff['t_coords'].attrs[tt]
            a=hf.create_dataset('z_coords',data=ff['z_coords'].value)
            for tt in ff['z_coords'].attrs.keys():
                a.attrs[tt]=ff['z_coords'].attrs[tt]
            a=hf.create_dataset('x_coords',data=ff['x_coords'].value)
            for tt in ff['x_coords'].attrs.keys():
                a.attrs[tt]=ff['x_coords'].attrs[tt]
            a=hf.create_dataset('y_coords',data=ff['y_coords'].value)
            for tt in ff['y_coords'].attrs.keys():
                a.attrs[tt]=ff['y_coords'].attrs[tt]
    except:
        outfile = None
        
    return outfile



