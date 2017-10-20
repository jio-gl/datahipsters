'''
Created on 04/07/2014

@author: jose
'''

import random
import md5
import sys
import time,datetime
import simplejson
from collections import Counter
#from concurrent import futures
import threading

from google.appengine.ext import db

# Param p : p-tuple of hashes for enhanced precision.
# Param q : q permutations for enhanced recall. 

class MinHashSQLModelException(Exception):
    pass

class SetIdNotInCluster(MinHashSQLModelException):
    pass

class AttributeRequired(MinHashSQLModelException):
    pass

class SQLModel(object):
    def __init__(self,table_name):
        self._table_name = table_name

    @classmethod
    def clear(cls):
        pass

    @classmethod
    def _put(cls, attr_tuple_list):
        pass
        

class PermutationSeed(SQLModel):
    p = None # int
    q = None # int
    seed = None # int
    def __init__(self, p=None, q=None):
        super(PermutationSeed, self).__init__(self.__class__.__name__)        
        if not p or not q:
            raise AttributeRequired(self.__class__.__name__)
        self.p = p
        self.q = q
         
        
    def __str__(self):
        return "p=%s, q=%s, seed=%s" % (self.p, self.q, self.seed)
    
    def put(self):
        self._put([('p',self.p),('q',self.q)])

# SetAllElemsPQ
class SetAllElemsPQ(db.Model):
    p = None # int
    q = None # int
    set_id = None # int
    elem_hash = None # str

    def __init__(self, p=None, q=None, set_id=None, elem_hash=None):
        super(SetAllElemsPQ, self).__init__(self.__class__.__name__)        
        if not p or not q or not set_id or not elem_hash:
            raise AttributeRequired(self.__class__.__name__) 
        self.p = p # int
        self.q = q # int
        self.set_id = set_id # int
        self.elem_hash = elem_hash # str
    
    
    def __str__(self):
        return "p=%s, q=%s, set_id=%s, elem_hash=%s" % (self.p, self.q, self.set_id, self.elem_hash)

    def put(self):
        self._put([('p',self.p),('q',self.q),('set_id',self.set_id),('elem_hash',self.elem_hash),])


# SetAllElems
class SetAllElems(db.Model):
    # using 
    set_id = None #int 
    elem_id = None #int
    
    def __init__(self, set_id=None, elem_id=None):
        super(SetAllElems, self).__init__(self.__class__.__name__)        
        if not set_id or not elem_id:
            raise AttributeRequired(self.__class__.__name__) 
        self.set_id = set_id #int 
        self.elem_id = elem_id #int
    
    def __str__(self):
        return "set_id=%s, elem_id=%s" % (self.set_id, self.elem_id)

    def put(self):
        self._put([('set_id',self.set_id),('elem_id',self.elem_id),])
    
    @classmethod
    def all_elems(cls, set_id, batch=1000):
        """
        Count *all* of the rows (without maxing out at 1000)
        """
        query = cls.all().filter('set_id = ', set_id).order('elem_id')

        rows = list(query.fetch(batch))
        count = 0
        while count % batch == 0:
            current_count = len(rows)
            if current_count == 0:
                break

            count += current_count
            rows = query.fetch(1000)
            for r in rows:
                yield r.elem_id

            if current_count == batch:
                last_key = rows[-1].elem_id
                                                
                query = query.filter('elem_id > ', last_key)
                rows = list(query.fetch(batch))


# MinimumElemRandomHash
class MinHashSetCluster(db.Model):
    q_iteration = None # int
    # joined by character -
    p_joined_min_elem_hashes = None # str
    # joined by character |
    joined_set_cluster = None # str

    def __init__(self, q_iteration=None, p_joined_min_elem_hashes=None, joined_set_cluster=None):
        super(MinHashSetCluster, self).__init__(self.__class__.__name__)        
        if not q_iteration or not p_joined_min_elem_hashes or not joined_set_cluster:
            raise AttributeRequired(self.__class__.__name__) 
        self.q_iteration = q_iteration # int
        # joined by character -
        self.p_joined_min_elem_hashes = p_joined_min_elem_hashes # str
        # joined by character |
        self.joined_set_cluster = joined_set_cluster # str
    
    def __str__(self):
        return "q_iteration=%s, p_joined_min_elem_hashes=%s, joined_set_cluster=%s" % (self.q_iteration, self.p_joined_min_elem_hashes, self.joined_set_cluster)

    # TODO: continue
    def put(self):
        self._put([('set_id',self.set_id),('elem_id',self.elem_id),])
    


# SetClusterHash
class SetMinHash(db.Model):
    q_iteration = db.IntegerProperty(required=True)
    set_id = db.IntegerProperty(required=True)
    # joined by character -
    p_joined_min_elem_hashes = db.StringProperty(required=True)

    def __init__(self, q_iteration=None, set_id=None, p_joined_min_elem_hashes=None):
        super(MinHashSetCluster, self).__init__(self.__class__.__name__)        
        if not q_iteration or not p_joined_min_elem_hashes or not set_id:
            raise AttributeRequired(self.__class__.__name__) 
    
    def __str__(self):
        return "q_iteration=%s, set_id=%s, p_joined_min_elem_hashes=%s" % (self.q_iteration, self.set_id, self.p_joined_min_elem_hashes)
    @classmethod
    def all_sets(cls, batch = 1000):
        """
        Count *all* of the rows (without maxing out at 1000)
        """
        query = cls.all().filter('q_iteration = ', 0).order('set_id')

        rows = list(query.fetch(batch))
        count = 0
        while count % batch == 0:
            current_count = len(rows)
            if current_count == 0:
                break

            count += current_count
            for r in rows:
                yield r.set_id

            if current_count == batch:
                last_key = rows[-1].set_id
                                                
                query = query.filter('q_iteration = ', 0).filter('set_id > ', last_key)
                rows = list(query.fetch(batch))

        #return count        


# int('aaabbbcccddd', self.__HASH_BIT_LENGTH/4)


class MinHash(object):
    
    _ITEM_HASH_SEP = '-'
    _USER_SEP      = '|'
    __HASH_BIT_LENGTH = 64
    _EMPTY_SET = 'EMPTY'
    _PUT_SLEEP_TIME = 0.05 #0.125
    
    def __init__(self, p=2, q=10, threshold=0.0):
        # p in [2,3,4]
        # q in [10..20]
        self.__p_precision = p
        self.__q_recall = q
        self.__threshold = threshold


    def reset(self):
        PermutationSeed.clear() #db.delete(db.Query(PermutationSeed))
        self.init_permutations_seeds()
        db.delete(db.Query(SetAllElemsPQ))        
        db.delete(db.Query(SetAllElems))
        db.delete(db.Query(MinHashSetCluster))        
        db.delete(db.Query(SetMinHash))       
        
    
    def _remove_from_joined_cluster(self, joinedsetcluster, setid):
        if joinedsetcluster==self._EMPTY_SET or joinedsetcluster == '':
            l = []
        else:
            l = [int(u) for u in joinedsetcluster.split(self._USER_SEP)]
        try:
            l.remove( setid )
        except ValueError:
            #pass
            raise SetIdNotInCluster('EXCEPTION: when removing set id from cluster not belonging to cluster.')
        ret = self._USER_SEP.join( [str(u) for u in l] )
        if ret == '':
            ret = self._EMPTY_SET
        return ret

        
    def _add_to_joined_cluster(self, joinedsetcluster, setid):
        if joinedsetcluster == '' or joinedsetcluster == self._EMPTY_SET :
            return str(setid) 
        else:
            #return joinedsetcluster + self._USER_SEP + str(setid)
            l = [int(u) for u in joinedsetcluster.split(self._USER_SEP)]
            if not setid in l:
                l.append( setid )
            return self._USER_SEP.join( [str(u) for u in l] )


    def _random_integer(self):
        return int(random.random()*10**(self.__HASH_BIT_LENGTH/4)) % 2**64
    
        
    def init_permutations_seeds(self):
        for p in range(self.__p_precision):
            for q in range(self.__q_recall):
                obj = PermutationSeed(
                                p = p,
                                q = q,
                                seed = self._random_integer()  
                                )
                obj.put()

            
    def get_seeds(self):
        seeds = PermutationSeed.all()
        self._seeds = {}
        for s in seeds:
            p,q,seed = s.p,s.q,s.seed
            if not q in self._seeds:
                self._seeds[q] = {}
            self._seeds[q][p] = seed


    def similarity(self, setA, setB , elemsA=None):
            
        if not elemsA:
            elemsA = set(SetAllElems.all_elems(set_id=setA)) #set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', setA).run()])
        elemsB = set(SetAllElems.all_elems(set_id=setB)) #set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', setB).run()])
            
        intersecting_elems = elemsA.intersection(elemsB)
        union_elems = elemsA.union(elemsB)
            
        return float(len(intersecting_elems)) / len(union_elems)
    
        
    def __get_n_neighbors( self, setid, n, q ):
        set_min_q_elems = list(db.Query(SetMinHash).filter('q_iteration =', q).filter('set_id =', setid))        
        if set_min_q_elems == []:
            raise MinHashModelException( 'Not enough set events!!! min events per set is %d' % self.__p_precision )
        # TODO: select which for simultaneous p future implemention
        set_min_q_elems = set_min_q_elems[0].p_joined_min_elem_hashes
        joinedsetcluster = list(db.Query(MinHashSetCluster).filter('p_joined_min_elem_hashes =', set_min_q_elems).fetch(1))[0]
        return [int(u) for u in joinedsetcluster.joined_set_cluster.split(self._USER_SEP) ]
    
    
    def get_n_neighbors( self, setid, n=None ):
        '''
        setid : long int
        n : small int
        '''
        similar_sets = []
        # repeat q times for better recall.
        for q in range(self.__q_recall):
            similar_sets += self.__get_n_neighbors(setid, n, q)
        # remove repetitions
        similar_reps = Counter(similar_sets)
        setid_reps = similar_reps[setid] 
        if setid_reps == 0:
            return [] 
        similar_reps = [(float(rep)/setid_reps,set_id) for set_id,rep in similar_reps.iteritems() ]
        similar_reps.sort()
        similar_reps.reverse()
        #return similar_reps
        similar_reps = [(rep,set_id) for rep,set_id in similar_reps if set_id!=setid]
        return n and similar_reps[:n] or similar_reps        
    
    
    def get_n_neighbors_concurrent( self, setid, n=None ):
        '''
        setid : long int
        n : small int
        '''
        # repeat q times for better recall.
        # http://www.tutorialspoint.com/python/python_multithreading.htm
        class NNeightborsThread (threading.Thread):
            def __init__(self, method, setid, n, q):
                threading.Thread.__init__(self)
                self.method = method
                self.setid = setid
                self.n = n
                self.q = q
            def run(self):
                self.result = self.method(self.setid,self.n,self.q)
        # Create new threads    
        threads = [NNeightborsThread(self.__get_n_neighbors, setid, n, q) for q in range(self.__q_recall)]
        map(lambda x: x.start(), threads)
        map(lambda x: x.join(), threads)

        similar_sets = [row for t in threads for row in t.result]
        
        # remove repetitions
        similar_reps = Counter(similar_sets)
        setid_reps = similar_reps[setid] 
        if setid_reps == 0:
            return [] 
        similar_reps = [(float(rep)/setid_reps,set_id) for set_id,rep in similar_reps.iteritems() ]
        similar_reps.sort()
        similar_reps.reverse()
        #return similar_reps
        similar_reps = [(rep,set_id) for rep,set_id in similar_reps if set_id!=setid]
        return n and similar_reps[:n] or similar_reps        
    
    
    def get_unweighted_recommendations(self, setid, n=None):
        
        user_items = set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', setid).run()])
        sim_sets = self.get_n_neighbors(setid)
        ret = set([])
        for sim,sim_set in sim_sets:
            sim_set_items = set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', sim_set).run()])
            # remove items already in set
            sim_set_items = sim_set_items.difference( user_items ) 
            ret = ret.union( sim_set_items )
            if n and len(ret) >= n:                
                ret = list(ret)
                ret.sort()
                return ret[:n]
        ret = list(ret)
        ret.sort()
        return ret 
    
    
    def get_recommendations(self, setid, n=None,neighbors_limit=None, threshold=None):
        
        if threshold:
            old_threshold = self.__threshold
            self.__threshold = threshold
              
        user_items = set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', setid).run()])
        sim_sets = self.get_n_neighbors(setid,n=neighbors_limit)
        ret = {}
        sim_set_total_weight = 0.0
        for weight, sim_set in sim_sets:
            sim_set_items = set([r.elem_id for r in db.Query(SetAllElems).filter('set_id =', sim_set).run()])
            # remove items already in set
            sim_set_items = sim_set_items.difference( user_items )
            sim_set_total_weight += weight
            for elem in sim_set_items:
                if not elem in ret:
                    ret[elem] = 0.0
                ret[elem] += weight
            if n and len(ret.keys()) >= n:                
                break
        ret = [(w/sim_set_total_weight,e) for e,w in ret.iteritems() if w/sim_set_total_weight >= self.__threshold]
        ret.sort()
        ret.reverse()
        if n and len(ret) >= n:                
            ret = ret[:n]
            
        if threshold:
            self.__threshold = old_threshold
            
        return ret 
    
    
    def _random_hash(self, elemid, seed):        
        myhash = md5.new()
        myhash.update( str(seed) )
        myhash.update( str(elemid) )
        return myhash.hexdigest()[:self.__HASH_BIT_LENGTH/4]
        #return int(, self.__HASH_BIT_LENGTH/4)


    def _random_hash_p_tuple(self, elemid, q):
        ret = []
        for p in self._seeds[q].keys():
            seed = self._seeds[q][p]
            ret.append( self._random_hash(elemid, seed) )
        return ret #[:-1]
        
    
    def str_to_int_hash(self, s_hash):
        return int(s_hash,self.__HASH_BIT_LENGTH/4)

        
    def int_to_str_hash(self, s_hash):
        return ('%.'+str(self.__HASH_BIT_LENGTH/4)+'x') % s_hash 

        
    def __store_set_allelems(self, setid, randomhashelem_list, q):
        # NO idempotency, nothing if repeteaded (setid,elemid)        
        # YES repeteaded events to avoid queries. 
        #if list(db.Query(SetAllElems).filter('set_id =', setid).filter('elem_hash =', randomhashelem_list[0]).fetch(1)) == []:
        # TODO: Paralellizable            
        
        models = [ SetAllElemsPQ(p = p_index, q = q, set_id = setid, elem_hash = randomhashelem,) for p_index, randomhashelem in enumerate(randomhashelem_list)]
        put_future = db.put_async(models)
        put_future.get_result() 
          
            #.put() # ES ASYNC ???
            
            
    def sets(self,batch=1000):
        return SetMinHash.all_sets(batch=batch)
        
            
    def elems(self, setid,batch=1000):
        return SetAllElems.all_elems(setid,batch=batch) 
        
            
    def __minhash(self, set_id, q):
        # return None is set is new
        minhash = ''
        # Parallelizable
        for p in range(self.__p_precision):
            elems = []
            elems = list(db.Query(SetAllElemsPQ).filter('q =', q).filter('p =', p).filter('set_id =', set_id).order('elem_hash').fetch(1))
            while len(elems) == 0:
                time.sleep(self._PUT_SLEEP_TIME)
                elems = list(db.Query(SetAllElemsPQ).filter('q =', q).filter('p =', p).filter('set_id =', set_id).order('elem_hash').fetch(1))
                #raise MinHashModelException( '__minhash(self, set_id=%d, q=%d)  p=%d, elems= in SetAllElemsPQ not available' % (set_id,q,p) )
                #return None
            minhash += elems[0].elem_hash + self._ITEM_HASH_SEP
        return minhash[:-1]


    def add_to_set(self, setid, elemid):
        # Reducing latency http://stackoverflow.com/questions/14415878/how-can-i-reduce-google-app-engine-datastore-latency
        '''
        setid : int
        elemid : int
        
        PRICING (https://developers.google.com/appengine/pricing):
        >>> 50000 * 0.01  (less than 1% of 50000 datastore operations take one add_to_set() )
        500.0
        >>> 6.0 / 100000  (google pricing 6 pesos arg (0.06 dollars) cada 100k datastore operations), argentine pesos per datastore operation
        6e-05
        >>> (6.0 / 100000) * 500  (total argentine pesos per add_to_set() )
        0.030000000000000002

        '''
        # store event tuple
        SetAllElems(
            set_id = setid,
            elem_id = elemid,
            ).put()
        # TODO: Parallelizable
        # update clusters
        for q in range(self.__q_recall):
            
            if q==8:                
                q=8
            
            # plain random hash for elem
            p_randomhashelem_list = self._random_hash_p_tuple(elemid,q)
            
            # retrieve old minhash and compute new one
            old_minhash_list = SetMinHash.get_minhash(q, setid) # list(db.Query(SetMinHash).filter('q_iteration =', q).filter('set_id =', setid).fetch(1))
            is_new_set = len(old_minhash_list) == 0            
            self.__store_set_allelems(setid, p_randomhashelem_list, q)
            time.sleep(self._PUT_SLEEP_TIME)
            new_minhash = self.__minhash(setid, q)
                
            # new set store                    
            if is_new_set:
                old_minhash = None
                minhash_record = SetMinHash(
                                                q_iteration = q,
                                                set_id = setid,
                                                p_joined_min_elem_hashes = new_minhash
                                                )
            else:
                old_minhash = old_minhash_list[0].p_joined_min_elem_hashes
                minhash_record = old_minhash_list[0]
                
            # new cluster
            new_joined_set_cluster_list = list(db.Query(MinHashSetCluster).filter('q_iteration =', q).filter('p_joined_min_elem_hashes =', new_minhash).fetch(1))
            new_cluster = len(new_joined_set_cluster_list) == 0
            if new_cluster:
                new_joined_set_cluster = MinHashSetCluster(
                                   q_iteration = q,
                                   p_joined_min_elem_hashes = new_minhash,
                                   joined_set_cluster = self._add_to_joined_cluster(self._EMPTY_SET,setid)
                                   )
            else:
                new_joined_set_cluster = new_joined_set_cluster_list[0]
                
            # is new hash
            new_joined_set_cluster.joined_set_cluster = self._add_to_joined_cluster(new_joined_set_cluster.joined_set_cluster, setid)                
            minhash_record.p_joined_min_elem_hashes = new_minhash
            if not is_new_set and new_minhash != old_minhash:
                # also remove from old cluster
                old_joined_set_cluster = list(db.Query(MinHashSetCluster).filter('q_iteration =', q).filter('p_joined_min_elem_hashes =', old_minhash).fetch(1))[0]
                old_joined_set_cluster.joined_set_cluster = self._remove_from_joined_cluster(old_joined_set_cluster.joined_set_cluster, setid)
                # notice: we don't delete empty clusters to avoid appengine delete() latency aprox x4 times put() latency 
                # http://code.google.com/status/appengine/detail/datastore/2014/06/17#ae-trust-detail-datastore-delete-latency
                if old_joined_set_cluster.joined_set_cluster == self._EMPTY_SET:
                    old_joined_set_cluster.delete()
                else:
                    old_joined_set_cluster.put()                
            new_joined_set_cluster.put()
            minhash_record.put()

            #if not is_new_set:    
                















