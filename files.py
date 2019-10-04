import numpy as np
import os
import h5py as hdf
from QButils.sanity import varcheck


def slowbeam_read(filename, ms=False):
    """
    Slowbeam reader.

    Input
    -----
    filename: a Slowbeam file
    ms (opt): boolean flag whether to read MS

    Output
    ------
    ref: dictionary of values

    """
    import numpy as np
    import struct

    ref = {}

    if filename[-3:] == '.gz':
        import gzip
        file = gzip.open(filename, 'rb')
    else:
        file = open(filename, 'rb')
    
    ref['filename'] = file.read(200)
    ref['freq'] = struct.unpack( "f", file.read(4) )[0]
    ref['nx'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['ny'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['nz'] = struct.unpack( "hxx", file.read(4) )[0]

    grid3d = ref['nz'] * ref['ny'] * ref['nx']

    shape3d = [ ref['nz'], ref['ny'], ref['nx'] ]

    ref['Z_eff'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )

    ref['doppler'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )

    ref['h_att'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )

    ref['g_att'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
                
    ref['hgt'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )

    if ms == True:
        ref['Z_ss'] = np.reshape( \
                    struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                    shape3d )

        ref['Z_ms'] = np.reshape( \
                    struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                    shape3d )

    file.close()

    return ref

def datread(filename, *args):
    """
    A QuickBeam reader. -- code from Ethan Nelson

    Input
    -----
    filename: a QuickBeam file

    Optional arguments
    ------------------
    *args: include only these variables

    Output
    ------
    ref: dictionary of values

    """
    import numpy as np
    import struct

    ref = {}

    if filename[-3:] == '.gz':
        import gzip
        file = gzip.open(filename, 'rb')
    else:
        file = open(filename, 'rb')

    ref['filename'] = file.read(200)
    ref['title'] = file.read(100)
    ref['sensor'] = file.read(20)
    ref['freq'] = struct.unpack( "f", file.read(4) )[0]
    ref['year'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['month'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['day'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['hour'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['minute'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['second'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['nx'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['ny'] = struct.unpack( "hxx", file.read(4) )[0]
    ref['deltax'] = struct.unpack( "f", file.read(4) )[0]
    ref['deltay'] = struct.unpack( "f", file.read(4) )[0]
    ref['nhgt'] = struct.unpack( "hxx", file.read(4) )[0]

    grid1d = ref['nhgt']
    grid2d = ref['ny'] * ref['nx']
    grid3d = ref['nhgt'] * ref['ny'] * ref['nx']

    shape1d = [ ref['nhgt'] ]
    shape2d = [ ref['ny'], ref['nx'] ]
    shape3d = [ ref['nhgt'], ref['ny'], ref['nx'] ]

    pos = 0

    if args and 'hgt' not in args:
        pos += 4*grid1d
    else:
        file.seek(pos,1)
        ref['hgt'] = np.reshape( \
                struct.unpack( str(grid1d)+"f", file.read(4*grid1d) ), \
                shape1d )
        pos = 0

    if args and 'lat' not in args:
        pos += 4*grid2d
    else:
        file.seek(pos,1)
        ref['lat'] = np.reshape( \
                struct.unpack( str(grid2d)+"f", file.read(4*grid2d) ), \
                shape2d )
        pos = 0

    if args and 'lon' not in args:
        pos += 4*grid2d
    else:
        file.seek(pos,1)
        ref['lon'] = np.reshape( \
                struct.unpack( str(grid2d)+"f", file.read(4*grid2d) ), \
                shape2d )
        pos = 0
    if args and 'sfcrain' not in args:
        pos += 4*grid2d
    else:
        file.seek(pos,1)
        ref['sfcrain'] = np.reshape( \
                struct.unpack( str(grid2d)+"f", file.read(4*grid2d) ), \
                shape2d )
        pos = 0

    if args and 'tempk' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['tempk'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    if args and 'Z_eff' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['Z_eff'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    if args and 'Z_ray' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['Z_ray'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    if args and 'h_atten' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['h_atten'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    if args and 'g_atten' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['g_atten'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    if args and 'Z_cor' not in args:
        pos += 4*grid3d
    else:
        file.seek(pos,1)
        ref['Z_cor'] = np.reshape( \
                struct.unpack( str(grid3d)+"f", file.read(4*grid3d) ), \
                shape3d )
        pos = 0

    file.close()

    if args:
        return {key: ref[key] for key in ref if key in args}
    else:
        return ref

def dattoh5(filename):
    x = datread(filename)
    ze = x['Z_eff']
    h = x['hgt']
    z = x['Z_cor']
    g = x['g_atten']
    a = x['h_atten']
    outputfile = os.path.splitext(filename)[0]+'.h5'
    with hdf.File(outputfile, 'w') as hf:
        hf.create_dataset('reflectivity', data=z.astype(np.float32))
        hf.create_dataset('height',data=h.astype(np.float32))
        hf.create_dataset('z_eff',data=ze.astype(np.float32))
        hf.create_dataset('atten',data=a.astype(np.float32))
        hf.create_dataset('gas_atten',data=g.astype(np.float32))
    os.system('rm '+filename)
    return outputfile
    
def SBtoh5(filename):
    x = slowbeam_read(filename)
    ze = np.squeeze(x['Z_eff'])
    ze[ze<-999]=-999
    h = np.squeeze(x['hgt'])
    g = np.squeeze(x['g_att'])
    a = np.squeeze(x['h_att'])
    dp = np.squeeze(x['doppler'])
    z = ze - a - g
    z[z<-999]=-999
    outputfile = os.path.splitext(filename)[0]+'.h5'
    with hdf.File(outputfile, 'w') as hf:
        hf.create_dataset('reflectivity', data=z.astype(np.float32))
        hf.create_dataset('height',data=h.astype(np.float32))
        hf.create_dataset('z_eff',data=ze.astype(np.float32))
        hf.create_dataset('atten',data=a.astype(np.float32))
        hf.create_dataset('gas_atten',data=g.astype(np.float32))
        hf.create_dataset('doppler',data=dp.astype(np.float32))
    os.system('rm '+filename)
    return outputfile    
    
def hclassfile(outfilename, varin):
    '''Routine to write an hclass file.
       Initial var dictionary is set to default RAMS values.
    
       Code reads in output file name, and the a list of tuples contaning:
                    1)hydrometeor to change 
                    2)parameter 
                    3)value

       Code returns filename to plug into 'settings.dat'

    '''
    #Default values in RAMS

    var={}
    var['#'] = np.arange(8)+1
    var['type'] = np.ones(8)
    var['col'] = np.ones(8)*-1
    var['phase'] = np.asarray([0,0,0,1,1,1,1,1])
    var['cp'] = np.zeros(8)
    var['dmin'] = np.ones(8)*-1
    var['dmax'] = np.ones(8)*-1
    var['apm'] = np.asarray([524,524,524,110.8,0.002739,0.496,157,471])
    var['bpm'] = np.asarray([3,3,3,2.91,1.74,2.4,3,3])
    var['rho'] = np.ones(8)*-1
    var['p1'] = np.ones(8)*-1
    var['p2'] = np.ones(8)*-1
    var['p3'] = np.asarray([4,4,2,2,2,2,2,2])
    var['nmom'] = np.ones(8)*2

    allkeys = ['#','type','col','phase','cp','dmin','dmax','apm','bpm','rho','p1','p2','p3','nmom']
    numclass = {'cloud':0,'drizzle':1,'rain':2,'pristine':3,'snow':4,'aggregates':5,'graupel':6,'hail':7}
    
    if not isinstance(varin[0], tuple):
        if varin == 'default':
            varin = ('cloud','#',1)
        varin = [varin,]

    err = varcheck(varin,'hydro')
    if err==True:
        for a in varin:
            hname,parname,val = a 
            ind = numclass[hname]
            var[parname][ind] = val

        for key in allkeys:
            if key != 'apm' and key != 'bpm':
                var[key]= var[key].astype(int)


        f = open(outfilename, 'wb')
        outlines = "#  type  col   phase  cp  dmin   dmax      apm    bpm   rho      p1   p2   p3  nmom  \n"
        for i in range(8):
            lin = ""
            for key in allkeys:
                if key == 'apm' or key == 'bpm':
                    fmtvar = str('{: f}'.format(var[key][i]))
                else:
                    fmtvar = str('{: d}'.format(var[key][i]))
                lin = lin+fmtvar+"\t"
            lin = lin + "\n"
            outlines = outlines + lin
        f.write(outlines)
        f.close()

        return (outfilename,True)
    else:
        return (outfilename,err)


def settingsfile(settingsfilename, **kwargs):
    '''Writes a settings file that will be used in the command line call to QuickBeam.
       
       User must specify a filename. '''

    defaultkwargs = {'sensor':'dtrain', 'freq':35.5, 'surface_radar':0, 'use_mie_tables':0,
             'use_gas_abs':1, 'sonde_format':2, 'do_ray':1, 'melt_lay':1, 'input_format':1, 
             'output_format':2, 'output_disp':0, 'k2':-1, 'hclassfile':'hclass.dat', 
             'miefile':'mie_table.dat'}

    var = ['sensor', 'freq', 'surface_radar', 'use_mie_tables', 'use_gas_abs', 'sonde_format', 'do_ray',
            'melt_lay', 'input_format', 'output_format', 'output_disp', 'k2', 'hclassfile', 'miefile']

    block = '! --------------------- comments below this line ----------------------\n'
    block = block + '! Parameters set to -1 defer to program settings \n'
    block = block + ' \n'
    block = block + '! Primary user definable parameters \n'
    block = block + '1   sensor      ! name of simulated sensor \n'
    block = block + '2   freq        ! radar frequency (GHz) \n'
    block = block + '3   surface_radar   ! surface=1, spaceborne=0 \n'
    block = block + '4   use_mie_tables  ! use a precomputed lookup table? \n'
    block = block + '5   use_gas_abs     ! include gaseous absorption? \n'
    block = block + '6   sonde_format    ! separate=1, combined=2, MLS=3, MLW=4, TRO=5 \n'
    block = block + '7   do_ray      ! calculate/output Rayleigh refl=1, not=0 \n'
    block = block + '8   melt_lay            ! melting layer model off=0, on=1 \n'
    block = block + '9   input_format    ! HDF5=1, Grads=2 \n'
    block = block + '10  output_format   ! full output=1, simple output=2 \n'
    block = block + '11  output_disp     ! output to display=1, file only=0 \n'
    block = block + '12  k2          ! |K|^2, -1=use frequency dependent default \n'
    block = block + ' \n'
    block = block + '! Input files: \n'
    block = block + '13 hclass_file          ! input hydrometer classes file \n'
    block = block + '14 mie_table_name   ! mie table file \n'

    for k in defaultkwargs.keys():
        if k not in kwargs:
            kwargs[k] = defaultkwargs[k]

    f = open(settingsfilename, 'wb')
    outlines = ""
    for i in range(14):
        outlines = outlines + str(i+1) + "  " + str(kwargs[var[i]]) + "\n"
    outlines = outlines + block
    f.write(outlines)
    f.close()

    return settingsfilename 

def runquick(runvar,removefiles=True):
    ''' Set up to run quickbeam given an input file and settings info.
    
        Code reads in 'runvar' which is meant to be a tuple containing:
            1. Input filename
            2. List of tuples of changes to the default microphysics file
            3. A dictionary, empty or with any changes to default settings file
            4. Filename for hclass file 
            5. Filename for settings file
            6. Filename for output file
            7. A run number

        Returns the outputfilename, or None if unsuccessful. 

        Should be run in the directory where Quickbeam executable is located.
    '''

    filename,micro,settings,hydrofile,settingsfilename,outputfile,numvar = runvar

    hfile,err =hclassfile(hydrofile,micro)

    if err == True:
        settings['hclassfile']=hfile
        setfile = settingsfile(settingsfilename, **settings)
        cmd = 'quickbeam_rams '+ filename +' '+ outputfile + '  ' + setfile
        os.system(cmd)
        if removefiles:
            os.system('rm '+setfile)
            os.system('rm '+hfile)
        h5file = dattoh5(outputfile)
        return h5file
    else:
        print filename, err
        return

