
def hclass(varin):
    columns = ['#','type','col','phase','cp','dmin','dmax','apm','bpm','rho','p1','p2','p3','nmom']
    hydro = ['cloud','drizzle','rain','pristine','snow','aggregates','graupel','hail']

    hname,parname,val = varin
    err = ''
    if type(hname) is not str or hname not in hydro:
        err = err + 'Bad Hydrometeor: '+str(hname)+' '
    if type(parname) is not str or parname not in columns:
        err = err + 'Bad Parameter Name: ' + str(parname) + ' '
    try:
        float(val)
    except:
        err = err + 'Bad Value: ' + str(val) + ' '
        pass
    if err == '':
        return True
    else:
        return err


def varcheck(varin,fil):
    errout = ''
    if fil == 'hydro':
        for a in varin:
            err = hclass(a)
            if err ==True:
                errout = errout + ''
            else:
                errout = errout + err 
    else:
        errout = 'bad value'


    if errout == '':
        return True
    else:
        return errout



