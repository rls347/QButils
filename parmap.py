'''
parmap.py -- Utility function that does a parallel Map (map/collect) of a Function onto a list of Items

Degree of parallelism is controlled by numPartitions parameter.
Parallel framework is chosen by mode parameter.
'''
from __future__ import print_function

import sys, os, time
import pysparkling, multiprocessing, cloudpickle, pickle

#from lambdaFns import lambdaMap    # THIS IS WHAT YOU SHOULD WRITE, the lambdaMap function (hiding details of parallel compute)


# Legal parallel execution modes
MODES = ['seq',         # sequential
         'par',         # pool.map using multiprocessing library
         'sparkling',   # spark map/collect using pysparkling library (multicore only)
         'spark',       # spark map/collect using spark cluster
         'dask',        # map/collect using dask distributed scheduler
         'lambda'       # map/collect using AWS Lambda functions via boto3 library, config dict contains credentials, etc.
        ]

def warn(s): print(s, file=sys.stderr)


def parmap(fn, items,
           mode='par',        # one of the six modes above
           numPartitions=8,   # number of partitions to split the list into, degree of parallelism
           config={}          # configuration dict for additional info:  credentials, etc.
          ):
    """Do a parallel Map of a function onto a list of items and then collect and return the list of results.
The function receives a list of values or S3 URL's and can perform I/O as a side effect (e.g. write files/objects and return their URLs).
    """
    if mode not in MODES:
        warn('parmap: Bad mode arg, using "par" (local multicore) instead: %s' % mode)
        mode = 'par'
#    warn('parmap %s: Running in mode %s with numPartitions %d' % (str(fn), mode, numPartitions))

    if mode == 'seq':
        return list(map(fn, items))

    elif mode == 'par':
        pool = multiprocessing.Pool(numPartitions)
        output = pool.map(fn, items)
        pool.close()
        return output

    elif mode == 'sparkling':
        try:
            sc = pysparkling.Context(multiprocessing.Pool(numPartitions), serializer=cloudpickle.dumps, deserializer=pickle.loads)
        except:
            raise 'parmap: mode sparkling, cannot get Context object'
        itemRDD = sc.parallelize(items, numPartitions)
        return itemRDD.map(fn).collect()

    elif mode == 'spark':
        return sparkMapCollect(fn, items, numPartitions, config)

    elif mode == 'dask':
        return daskMap(fn, items, numPartitions, config)

    elif mode == 'lambda':
        pass
#        return lambdaMap(fn, items, )


def sparkMapCollect(fn, items, numPartitions, config):
    '''Do a spark map/collect for fn applied to items, using numPartitions and config dict.'''
    import pyspark
    try:
        appName = config['appName']
    except:
        appName = None
    try:
        sc = pyspark.SparkContext(appName=appName)
    except:
        raise 'parmap; mode spark, cannot get Context object'

    itemRDD = sc.parallelize(items, numPartitions)
    return itemRDD.map(fn).collect()


def daskMap(fn, items, numPartitions, config):
    '''Do a dask parallel map for fn applied to items, using numPartitions and config dict.'''
    from dask.distributed import Client
    try:
        clientUrl = config['clientUrl']
    except:
        clientUrl = None
    if clientUrl is  not None:
        try:
            client = Client(clientUrl)
            warn('parmap: mode dask, using cluster scheduler at: %s' % clientUrl)
        except:
            raise 'parmap: mode dask, could not open cluster client at: %s' % clientUrl
    else:
        client = Client()     # makes default LocalCluster using nWorkers = nCores
        warn('parmap: mode dask, no client URL so using LocalCluster()')
    futures = client.map(fn, items, key='parmap', retries=1)
    return client.gather(futures)


# Tests follow.

class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
        warn('timer (sec): ' + str(self.interval))


def test(fn, n):
    from math import pow
    items = list(range(n))
    for mode in MODES:
        if mode == 'lambda': continue
        #if mode == 'spark' or mode == 'lambda': continue    # not testing these modes on Mac
        with Timer():
            results = parmap(fn, items, mode=mode, numPartitions=8, config={'appName': 'parmap_cubed_test'})
        print(mode,results[-1])

def cubed(i):
    from math import pow
    return pow(i,3)


if __name__ == '__main__':
    from math import factorial
    fn = sys.argv[1]
    n = int(sys.argv[2])
    if fn == 'cubed':
        fn = cubed
    else:
        fn = factorial
    test(fn, n)


# python parmap.py cubed 100000
# python parmap.py factorial 10000

