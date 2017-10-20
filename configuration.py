'''
Created on 20/11/2014

@author: jose
'''

class DHConfiguration(object):
    '''
    classdocs
    '''

    DEBUG              = False
    
    P_HASH_CARDINALITY = 1
    Q_HASH_SAMPLES     = 10
    N_DEFAULT_RESULTS  = 5
    
    DEFER_ACTIONS      = True
    
    USE_URL_MEMCACHE   = False
    URL_CACHE_LIFE     = 60 * 60 * 2 #seconds
    
    USE_DATASTORE_CACHE= True
    DATASTORE_CACHE_LIFE= 0 # 0 means no end life until dropped from memory


    def __init__(self):
        '''
        Constructor
        '''
        pass
        