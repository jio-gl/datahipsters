'''
Created on 08/06/2014

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
import logging
import hashlib

from google.appengine.ext import deferred
from google.appengine.runtime import DeadlineExceededError
from google.appengine.api import memcache

from google.appengine.ext import db
from configuration import DHConfiguration
from mapper import Mapper


# Param p : p-tuple of hashes for enhanced precision.
# Param q : q permutations for enhanced recall. 

class MinHashModelException(Exception):
    pass

class SetIdNotInCluster(MinHashModelException):
    pass

class NoUserDataYet(MinHashModelException):
    pass


class DataHipstersUser2(db.Model):
    _app = 'datahipsters'
    client_id = db.StringProperty(required=False)
    action_token = db.StringProperty(required=False)
    reco_token = db.StringProperty(required=False)
    actions_left = db.IntegerProperty(required=False)
    recos_left = db.IntegerProperty(required=False)
    user_level = db.IntegerProperty(required=False)
    widget_recommended_items = db.StringProperty(required=False,multiline=True)
    widget_recommend_item_to = db.StringProperty(required=False,multiline=True)
    widget_similar_items = db.StringProperty(required=False,multiline=True)
    widget_similar_users = db.StringProperty(required=False,multiline=True)
    widget_top_items = db.StringProperty(required=False,multiline=True)
    widget_top_users = db.StringProperty(required=False,multiline=True)
    
    def __str__(self):
        import xml.dom.minidom
        xml_txt = self.to_xml() 
        xml = xml.dom.minidom.parseString(xml_txt) # or xml.dom.minidom.parseString(xml_string)
        pretty_xml_as_string = xml.toprettyxml()
        return pretty_xml_as_string #.toprettyxml() #str(self.to_xml())
    #    return "user_id=%s, secret_token=%s, adds_left=%s, recos_left=%s" % (self.user_id, self.secret_token, self.adds_left, self.recos_left)



class PermutationSeed(db.Model):
    _app = 'datahipsters'
    p = db.IntegerProperty(required=True)
    q = db.IntegerProperty(required=True)
    seed = db.IntegerProperty(required=True)    
    def __str__(self):
        return "p=%s, q=%s, seed=%s" % (self.p, self.q, self.seed)

# SetAllElemsPQ
class SetAllElemsPQ(db.Model):
    # DEPRECATED
    p = db.IntegerProperty(required=True)
    q = db.IntegerProperty(required=True)
    set_id = db.IntegerProperty(required=True)
    elem_hash = db.StringProperty(required=True)
    def __str__(self):
        return "p=%s, q=%s, set_id=%s, elem_hash=%s" % (self.p, self.q, self.set_id, self.elem_hash)


# SetAllElemsPQ2
class SetAllElemsPQ2(db.Model):
    p = db.IntegerProperty(required=True)
    q = db.IntegerProperty(required=True)
    set_id = db.StringProperty(required=True)
    elem_hash = db.StringProperty(required=True)
    def __str__(self):
        return "p=%s, q=%s, set_id=%s, elem_hash=%s" % (self.p, self.q, self.set_id, self.elem_hash)


# SetAllElems2
class SetAllElems(db.Model):
    # Use of Ints DEPRECATED!!!, both are StringProperties
    # using 
    set_id = db.IntegerProperty(required=True)
    elem_id = db.IntegerProperty(required=True)
    def __str__(self):
        return "set_id=%s, elem_id=%s" % (self.set_id, self.elem_id)


# SetAllElems2
class SetAllElems2(db.Model):
    # Use of Ints DEPRECATED!!!, both are StringProperties
    # using 
    set_id = db.StringProperty(required=True)
    elem_id = db.StringProperty(required=True)
    def __str__(self):
        return "set_id=%s, elem_id=%s" % (self.set_id, self.elem_id)


# SetCount
class SetCount(db.Model):
    set_id = db.StringProperty(required=True)
    count = db.IntegerProperty(required=True)
    client_id = db.StringProperty(required=True)
    bucket_id = db.StringProperty(required=True)
    def __str__(self):
        return "client_id=%s, bucket_id=%s, set_id=%s, count=%s" % (self.client_id, self.bucket_id, self.set_id, self.count)


# SetCount
class ElemCount(db.Model):
    elem_id = db.StringProperty(required=True)
    count = db.IntegerProperty(required=True)
    client_id = db.StringProperty(required=True)
    bucket_id = db.StringProperty(required=True)
    def __str__(self):
        return "client_id=%s, bucket_id=%s, elem_id=%s, count=%s" % (self.client_id, self.bucket_id, self.elem_id, self.count)




# MinimumElemRandomHash
class MinHashSetCluster2(db.Model):
    q_iteration = db.IntegerProperty(required=True)
    # joined by character -
    p_joined_min_elem_hashes = db.StringProperty(required=True) 
    # joined by character |
    #joined_set_cluster = db.StringProperty(required=True) # limited to 500 bytes, error
    joined_set_cluster = db.TextProperty(required=True) # limited to 500 bytes, error
    def __str__(self):
        return "q_iteration=%s, p_joined_min_elem_hashes=%s, joined_set_cluster=%s" % (self.q_iteration, self.p_joined_min_elem_hashes, self.joined_set_cluster)



# SetClusterHash
class SetMinHash(db.Model):
    # DEPRECATED
    q_iteration = db.IntegerProperty(required=True)
    set_id = db.IntegerProperty(required=True)
    # joined by character -
    p_joined_min_elem_hashes = db.StringProperty(required=True)
    def __str__(self):
        return "q_iteration=%s, set_id=%s, p_joined_min_elem_hashes=%s" % (self.q_iteration, self.set_id, self.p_joined_min_elem_hashes)


# SetClusterHash
class SetMinHash2(db.Model):
    q_iteration = db.IntegerProperty(required=True)
    set_id = db.StringProperty(required=True)
    # joined by character -
    p_joined_min_elem_hashes = db.StringProperty(required=True)
    def __str__(self):
        return "q_iteration=%s, set_id=%s, p_joined_min_elem_hashes=%s" % (self.q_iteration, self.set_id, self.p_joined_min_elem_hashes)


class ResetKindMapper(Mapper):
    # Subclasses can replace this with a list of (property, value) tuples to filter by.
    FILTERS = []
    def __init__(self, kind):
        super(ResetKindMapper, self).__init__()
        self.KIND = kind
        
    def map(self, entity):
        """Updates a single entity. Implementers should return a tuple containing two iterables (to_update, to_delete)."""
        return ([], [entity])



class RemoveElemSetAllElems2Mapper(Mapper):
    # Subclasses should replace this with a model class (eg, model.Person).
    KIND = SetAllElems2
    # Subclasses can replace this with a list of (property, value) tuples to filter by.
    FILTERS = []
    def __init__(self, elem_id):
        super(RemoveElemSetAllElems2Mapper, self).__init__()
        self.FILTERS.append( ('elem_id', elem_id) )
        
    def map(self, entity):
        """Updates a single entity. Implementers should return a tuple containing two iterables (to_update, to_delete)."""
        return ([], [entity])

class RemoveElemSetAllElemsPQ2Mapper(Mapper):
    # Subclasses should replace this with a model class (eg, model.Person).
    KIND = SetAllElemsPQ2
    # Subclasses can replace this with a list of (property, value) tuples to filter by.
    FILTERS = []
    def __init__(self, elem_hash):
        super(RemoveElemSetAllElemsPQ2Mapper, self).__init__()
        self.FILTERS.append( ('elem_hash', elem_hash) )
        
    def map(self, entity):
        """Updates a single entity. Implementers should return a tuple containing two iterables (to_update, to_delete)."""
        return ([], [entity])

class RemoveSetSetAllElems2Mapper(Mapper):
    # Subclasses should replace this with a model class (eg, model.Person).
    KIND = SetAllElems2
    # Subclasses can replace this with a list of (property, value) tuples to filter by.
    FILTERS = []
    def __init__(self, set_id):
        super(RemoveSetSetAllElems2Mapper, self).__init__()
        self.FILTERS.append( ('set_id', set_id) )
        
    def map(self, entity):
        """Updates a single entity. Implementers should return a tuple containing two iterables (to_update, to_delete)."""
        return ([], [entity])

class RemoveSetSetAllElemsPQ2Mapper(Mapper):
    # Subclasses should replace this with a model class (eg, model.Person).
    KIND = SetAllElemsPQ2
    # Subclasses can replace this with a list of (property, value) tuples to filter by.
    FILTERS = []
    def __init__(self, set_id):
        super(RemoveSetSetAllElemsPQ2Mapper, self).__init__()
        logging.info("self.FILTERS.append( ('set_id', set_id) ) " + set_id)
        self.FILTERS.append( ('set_id', set_id) )
        
    def map(self, entity):
        """Updates a single entity. Implementers should return a tuple containing two iterables (to_update, to_delete)."""
        return ([], [entity])






class MinHash(object):
    
    _ITEM_HASH_SEP = '-'
    _USER_SEP      = '|'
    _COUNT_SEP     = ':'
    _ID_MULTIPLEXOR_SEP     = '_'
    _SET_ELEM_PREFIX     = 'se' + _ID_MULTIPLEXOR_SEP
    _ELEM_SET_PREFIX     = 'es' + _ID_MULTIPLEXOR_SEP
    _HASH_BIT_LENGTH = 64
    _MAX_STRING_LEN = 256
    _EMPTY_SET = 'EMPTY'
    _PUT_SLEEP_TIME = 0.075 #0.05 #0.125
    _app = 'datahipsters'
    _MAX_PER_PAGE = 10000
    _MAX_SIMULTANEOUS_DELETES = 100
    
    
    def get_max_string_len(self):
        return self._MAX_STRING_LEN
    
    
    def __init__(self, p=1, q=10, threshold=0.0):
        # p in [2,3,4]
        # q in [10..20]
        self.__p_precision = p
        self.__q_recall = q
        self.__threshold = threshold


    def reset(self, reset_seeds=False):
        if reset_seeds:
            db.delete(db.Query(PermutationSeed))
            #ResetKindMapper(PermutationSeed).run()
            time.sleep(self._PUT_SLEEP_TIME)
            self.init_permutations_seeds()
            
        ResetKindMapper(SetAllElemsPQ2).run()
        #db.delete(db.Query(SetAllElemsPQ2))
        ResetKindMapper(SetAllElems2).run()
        #db.delete(db.Query(SetAllElems2))
        ResetKindMapper(MinHashSetCluster2).run()
        #db.delete(db.Query(MinHashSetCluster2))
        ResetKindMapper(SetMinHash2).run()
        #db.delete(db.Query(SetMinHash2))
           
        # USer
        #db.delete(db.Query(DataHipstersUser))
        #db.delete(db.Query(DataHipstersUser2))
        
        # Counts
        ResetKindMapper(SetCount).run()
        #db.delete(db.Query(SetCount))
        ResetKindMapper(ElemCount).run()
        #db.delete(db.Query(ElemCount))


    def remove_elem_symm_ident( self, elemid, client_id, bucket_id):

        try:

            self.get_seeds()
    
            # remove count        
            db.delete( db.Query(ElemCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).filter('elem_id =', elemid).fetch(1) )
                
            elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
            self.remove_elem_set_elem( elemid )
            self.remove_set_elem_set( elemid )
            
        except DeadlineExceededError:
            # Queue a new task to pick up where we left off.
            deferred.defer(self.remove_elem_symm_ident, elemid, client_id, bucket_id )


    def remove_set_symm_ident( self, setid, client_id, bucket_id):

        try:

            self.get_seeds()
    
            # remove count        
            db.delete( db.Query(SetCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).filter('set_id =', setid).fetch(1) )
                
            setid = self.pack_client_bucket(setid,client_id,bucket_id)
            self.remove_set_set_elem( setid )
            self.remove_elem_elem_set( setid )
            
        except DeadlineExceededError:
            # Queue a new task to pick up where we left off.
            deferred.defer(self.remove_set_symm_ident, setid, client_id, bucket_id )
        
        

    #def remove_elem_set_elem_ident( self, elemid, n, client_id, bucket_id, randomized=True ):    
    #    elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
    #    self.remove_elem_set_elem( elemid )

    #def remove_elem_elem_set_ident( self, elemid, n, client_id, bucket_id, randomized=True ):    
    #    elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
    #    self.remove_elem_elem_set( elemid )

    def remove_elem_set_elem( self, elemid ):
        elemid = self._SET_ELEM_PREFIX + elemid 
        self.remove_elem( elemid )
    
    def remove_elem_elem_set( self, elemid ):
        elemid = self._ELEM_SET_PREFIX + elemid
        self.remove_elem( elemid )

    def remove_set_set_elem( self, setid ):
        setid = self._SET_ELEM_PREFIX + setid
        self.remove_set( setid )
    
    def remove_set_elem_set( self, setid ):
        setid = self._ELEM_SET_PREFIX + setid
        self.remove_set( setid )
        

    def remove_elem(self, elem_id):

        #RemoveElemSetAllElems2Mapper(elem_id).run()
        entities = list(db.Query(SetAllElems2).filter('elem_id =', elem_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))
        while entities != []:
            db.delete( entities )
            entities = list(db.Query(SetAllElems2).filter('elem_id =', elem_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))
        
        for q in range(self.__q_recall):
            
            randomhashelem_list = self._random_hash_p_tuple(elem_id,q)
            for _, randomhashelem in enumerate(randomhashelem_list): # p=1
                #RemoveElemSetAllElemsPQ2Mapper(randomhashelem).run()
                #db.delete( db.Query(SetAllElemsPQ2).filter('elem_hash =', randomhashelem).fetch( self._MAX_SIMULTANEOUS_DELETES ) )
                entities = list(db.Query(SetAllElemsPQ2).filter('elem_hash =', randomhashelem).fetch( self._MAX_SIMULTANEOUS_DELETES ))
                while entities != []:
                    db.delete( entities )
                    entities = list(db.Query(SetAllElemsPQ2).filter('elem_hash =', randomhashelem).fetch( self._MAX_SIMULTANEOUS_DELETES ))

            
    def remove_set(self, set_id):
        
        #RemoveSetSetAllElems2Mapper(set_id).run()
        #db.delete( db.Query(SetAllElems2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ) )
        entities = list(db.Query(SetAllElems2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))
        while entities != []:
            db.delete( entities )
            entities = list(db.Query(SetAllElems2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))
            
        #RemoveSetSetAllElemsPQ2Mapper(set_id).run()
        #db.delete( db.Query(SetAllElemsPQ2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ) )
        entities = list(db.Query(SetAllElemsPQ2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))
        while entities != []:
            db.delete( entities )
            entities = list(db.Query(SetAllElemsPQ2).filter('set_id =', set_id).fetch( self._MAX_SIMULTANEOUS_DELETES ))

        for q in range(self.__q_recall):
            
            old_minhash_list = list(db.Query(SetMinHash2).filter('q_iteration =', q).filter('set_id =', set_id).fetch(1))            
            self.delete_cache_setminhash(q, set_id)
                    
            if len(old_minhash_list) > 0:
                            
                old_minhash = old_minhash_list[0].p_joined_min_elem_hashes 
                            
                old_joined_set_cluster = list(db.Query(MinHashSetCluster2).filter('q_iteration =', q).filter('p_joined_min_elem_hashes =', old_minhash).fetch(1))[0]
                old_joined_set_cluster.joined_set_cluster = self._remove_from_joined_cluster(old_joined_set_cluster.joined_set_cluster, set_id)
                
                # notice: we don't delete empty clusters to avoid appengine delete() latency aprox x4 times put() latency 
                # http://code.google.com/status/appengine/detail/datastore/2014/06/17#ae-trust-detail-datastore-delete-latency
                if old_joined_set_cluster.joined_set_cluster == self._EMPTY_SET:
                    old_joined_set_cluster.delete()
                    self.delete_cache_minhashsetcluster(q, old_minhash)
                else:
                    old_joined_set_cluster.put()        
                    self.update_cache_minhashsetcluster(q, old_minhash, [old_joined_set_cluster])        

                old_minhash_list[0].delete()
                        
    
    
    def _remove_from_joined_cluster(self, joinedsetcluster, setid):
        # sanitize setid string
        setid = setid.replace(self._USER_SEP,'')
        if joinedsetcluster==self._EMPTY_SET or joinedsetcluster == '':
            l = []
        else:
            l = [u for u in joinedsetcluster.split(self._USER_SEP)]
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
            l = [u for u in joinedsetcluster.split(self._USER_SEP)]
            if not setid in l:
                l.append( setid )
            return self._USER_SEP.join( [str(u) for u in l] )


    def _random_integer(self):
        return int(random.random()*10**(self._HASH_BIT_LENGTH/4)) % 2**64
    
        
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
            elemsA = set(SetAllElems2.all_elems(set_id=setA)) #set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', setA).run()])
        elemsB = set(SetAllElems2.all_elems(set_id=setB)) #set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', setB).run()])
            
        intersecting_elems = elemsA.intersection(elemsB)
        union_elems = elemsA.union(elemsB)
            
        return float(len(intersecting_elems)) / len(union_elems)
    
    # MemCache Section
    
    def get_setminhash(self, q, setid):
        if DHConfiguration.USE_DATASTORE_CACHE:
            key = str( (q,setid) )
            data = memcache.get( key )
            if data is None:
                data = list(db.Query(SetMinHash2).filter('q_iteration =', q).filter('set_id =', setid))
                memcache.add(key, data, DHConfiguration.DATASTORE_CACHE_LIFE)
        else:
            data = list(db.Query(SetMinHash2).filter('q_iteration =', q).filter('set_id =', setid))
        return data
        
    def update_cache_setminhash(self, q, setid, set_min_q_elems):
        if not DHConfiguration.USE_DATASTORE_CACHE:
            return
        key = str( (q,setid) )
        memcache.set(key, set_min_q_elems, DHConfiguration.DATASTORE_CACHE_LIFE)
        
    def delete_cache_setminhash(self, q, setid):
        if not DHConfiguration.USE_DATASTORE_CACHE:
            return
        key = str( (q,setid) )
        memcache.delete(key)

    def get_minhashsetcluster(self, q, minhash):
        if DHConfiguration.USE_DATASTORE_CACHE:
            key = str( (q,minhash) )
            data = memcache.get( key )
            if data is None:
                data = list(db.Query(MinHashSetCluster2).filter('p_joined_min_elem_hashes =', minhash).fetch(1))
                memcache.add(key, data, DHConfiguration.DATASTORE_CACHE_LIFE)
        else:
            data = list(db.Query(MinHashSetCluster2).filter('p_joined_min_elem_hashes =', minhash).fetch(1))
        return data
        
    def update_cache_minhashsetcluster(self, q, minhash, set_min_q_elems):
        if not DHConfiguration.USE_DATASTORE_CACHE:
            return
        key = str( (q,minhash) )
        memcache.set(key, set_min_q_elems, DHConfiguration.DATASTORE_CACHE_LIFE)
        
    def delete_cache_minhashsetcluster(self, q, minhash):
        if not DHConfiguration.USE_DATASTORE_CACHE:
            return
        key = str( (q,minhash) )
        memcache.delete(key)
        

    # End MemCache Section
        
        
    def __get_n_neighbors( self, setid, n, q ):
        
        set_min_q_elems = self.get_setminhash(q, setid)        
        if set_min_q_elems == []:
            raise NoUserDataYet( 'Not enough set events!!! min events per set is %d' % self.__p_precision )
        # TODO: select which for simultaneous p future implemention
        p_joined_min_elem_hashes = set_min_q_elems[0].p_joined_min_elem_hashes
        try:
            joinedsetcluster = self.get_minhashsetcluster(q, p_joined_min_elem_hashes)[0]
        except:
            return []
        return [u for u in joinedsetcluster.joined_set_cluster.split(self._USER_SEP) ]
    
    
    def pack_client_bucket(self, someid,client_id,bucket_id):
        someid = someid.replace(self._ID_MULTIPLEXOR_SEP,'')
        client_id = client_id.replace(self._ID_MULTIPLEXOR_SEP,'')
        bucket_id = bucket_id.replace(self._ID_MULTIPLEXOR_SEP,'')
        return someid + self._ID_MULTIPLEXOR_SEP + client_id + self._ID_MULTIPLEXOR_SEP + bucket_id
    
    
    def get_n_neighbors_set_elem_ident( self, setid, n, client_id, bucket_id, randomized=True ):    
        setid = self.pack_client_bucket(setid,client_id,bucket_id)
        results = self.get_n_neighbors_set_elem( setid, n=n, randomized=randomized )
        return [(rep,set_id.split(self._ID_MULTIPLEXOR_SEP)[0]) for rep,set_id in results]
    
    
    def get_n_neighbors_elem_set_ident( self, elemid, n, client_id, bucket_id, randomized=True ):    
        elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
        results = self.get_n_neighbors_elem_set( elemid, n=n, randomized=randomized )
        return [(rep,set_id.split(self._ID_MULTIPLEXOR_SEP)[0]) for rep,set_id in results]
    
    
    def get_n_neighbors_set_elem( self, setid, n=None, randomized=True ):
        setid = self._SET_ELEM_PREFIX + setid
        results = self._get_n_neighbors( setid, n=n, randomized=randomized )
        return [(rep,set_id[len(self._SET_ELEM_PREFIX):]) for rep,set_id in results]

    
    def get_n_neighbors_elem_set( self, elemid, n=None, randomized=True ):
        elemid = self._ELEM_SET_PREFIX + elemid
        results = self._get_n_neighbors( elemid, n=n, randomized=randomized )
        return [(rep,set_id[len(self._ELEM_SET_PREFIX):]) for rep,set_id in results]


    def _choose_randomized(self, similar_reps, n):
        
        ret = []
        total = float(sum( rep for rep,_ in similar_reps ))
        if total == 0.0:
            return []
        
        similar_reps = [(rep/total,set_id) for rep,set_id in similar_reps]
        
        modificator = 1.0
        while len(ret) < n and similar_reps != []:
            
            rand_flt = random.random()
            new_similar_reps = []
            for rep,set_id in similar_reps:
                
                if rep < rand_flt*modificator:
                    ret.append( (rep,set_id) )
                else:
                    new_similar_reps.append( (rep,set_id) )
                    
            modificator *= 2.0
            similar_reps = new_similar_reps
        
        return ret 
        
    
    def _get_n_neighbors( self, setid, n=None, randomized=True ):
        '''
        setid : long int
        n : small int
        '''
        
        if not n:
            n = self._MAX_PER_PAGE
        
        similar_sets = []
        # repeat q times for better recall.
        for q in range(self.__q_recall):
            similar_sets += self.__get_n_neighbors(setid, n, q)
            
            #if len(similar_sets) > self._MAX_PER_PAGE:
            #    similar_sets = similar_sets[:self._MAX_PER_PAGE]
            #    break
            
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
        # random flag
        if randomized:
            similar_reps = self._choose_randomized(similar_reps, n)
        #similar_reps = [(rep,set_id) for rep,set_id in similar_reps]
        
        if len(similar_reps) == 0:
            raise NoUserDataYet('setid has no neighbors yet!')
                
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
    
    
    def get_unweighted_recommendations(self, setid, n=None, concurrent=False):
        
        user_items = set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', setid).run()])
        if not concurrent:
            sim_sets = self._get_n_neighbors(setid)
        else:
            sim_sets = self.get_n_neighbors_concurrent(setid)
        ret = set([])
        for sim,sim_set in sim_sets:
            sim_set_items = set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', sim_set).run()])
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
    
    
    def get_set_elem_recommendations(self, setid, n=None,neighbors_limit=None, threshold=None, concurrent=False,randomized=True):        
        setid = self._SET_ELEM_PREFIX + setid
        results = self._get_recommendations(setid, n=n,neighbors_limit=neighbors_limit, threshold=threshold, concurrent=concurrent,randomized=randomized)
        return [(rep,set_id[len(self._SET_ELEM_PREFIX):]) for rep,set_id in results]
        
    
    def get_set_elem_recommendations_ident(self, setid, n=None,neighbors_limit=None, threshold=None, concurrent=False,client_id=None,bucket_id=None,randomized=True):
        setid = self.pack_client_bucket(setid,client_id,bucket_id)
        results = self.get_set_elem_recommendations(setid, n=n,neighbors_limit=neighbors_limit, threshold=threshold, concurrent=concurrent, randomized=randomized)
        return [(rep,set_id.split(self._ID_MULTIPLEXOR_SEP)[0]) for rep,set_id in results]
        
    
    def get_elem_set_recommendations(self, elemid, n=None,neighbors_limit=None, threshold=None, concurrent=False,randomized=True):        
        elemid = self._ELEM_SET_PREFIX + elemid
        results = self._get_recommendations(elemid, n=n,neighbors_limit=neighbors_limit, threshold=threshold, concurrent=concurrent,randomized=randomized)
        return [(rep,set_id[len(self._ELEM_SET_PREFIX):]) for rep,set_id in results]
        
    
    def get_elem_set_recommendations_ident(self, elemid, n=None,neighbors_limit=None, threshold=None, concurrent=False,client_id=None,bucket_id=None,randomized=True):
        elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
        results = self.get_elem_set_recommendations(elemid, n=n,neighbors_limit=neighbors_limit, threshold=threshold, concurrent=concurrent, randomized=randomized)
        return [(rep,set_id.split(self._ID_MULTIPLEXOR_SEP)[0]) for rep,set_id in results]
        
    
    def _get_recommendations(self, setid, n=None,neighbors_limit=None, threshold=None, concurrent=False, randomized=True):
        
        if threshold:
            old_threshold = self.__threshold
            self.__threshold = threshold
              
        user_items = set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', setid).run()])
        if not concurrent:
            sim_sets = self._get_n_neighbors(setid,n=neighbors_limit, randomized=False)
        else:
            sim_sets = self.get_n_neighbors_concurrent(setid,n=neighbors_limit)
            
        ret = {}
        sim_set_total_weight = 0.0
        for weight, sim_set in sim_sets:
            sim_set_items = set([r.elem_id for r in db.Query(SetAllElems2).filter('set_id =', sim_set).run()])
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
        
        if len(ret) == 0:
            raise NoUserDataYet('setid has no neighbors with new items yet!')
        
        if randomized:
            ret = self._choose_randomized(ret, n)
        
        if n and len(ret) >= n:                
            ret = ret[:n]
            
        if threshold:
            self.__threshold = old_threshold
            
        return ret 
    
    
    def _random_hash(self, elemid, seed):        
        myhash = md5.new()
        myhash.update( str(seed) )
        myhash.update( str(elemid) )
        return myhash.hexdigest()[:self._HASH_BIT_LENGTH/4]
        #return int(, self._HASH_BIT_LENGTH/4)


    def _random_hash_p_tuple(self, elemid, q):
        ret = []
        #logging.warn(str(self._seeds))
        for p in self._seeds[q].keys():
            seed = self._seeds[q][p]
            ret.append( self._random_hash(elemid, seed) )
        return ret #[:-1]
        
    
    def str_to_int_hash(self, s_hash):
        return int(s_hash,self._HASH_BIT_LENGTH/4)

        
    def int_to_str_hash(self, s_hash):
        return ('%.'+str(self._HASH_BIT_LENGTH/4)+'x') % s_hash 

        
    def __store_set_allelems(self, setid, randomhashelem_list, q):
        # NO idempotency, nothing if repeteaded (setid,elemid)        
        # YES repeteaded events to avoid queries. 
        #if list(db.Query(SetAllElems2).filter('set_id =', setid).filter('elem_hash =', randomhashelem_list[0]).fetch(1)) == []:
        # TODO: Paralellizable            
        
        models = [ SetAllElemsPQ2(p = p_index, q = q, set_id = setid, elem_hash = randomhashelem,) for p_index, randomhashelem in enumerate(randomhashelem_list)]
        put_future = db.put_async(models)
        put_future.get_result() 
          
            #.put() # ES ASYNC ???
            
            
    def sets(self,batch=1000):
        return SetMinHash2.all_sets(batch=batch)
        
            
    def elems(self, setid,batch=1000):
        return SetAllElems2.all_elems(setid,batch=batch) 
        
            
    def __minhash(self, set_id, q):
        # return None is set is new
        minhash = ''
        # Parallelizable
        for p in range(self.__p_precision):
            elems = []
            elems = list(db.Query(SetAllElemsPQ2).filter('q =', q).filter('p =', p).filter('set_id =', set_id).order('elem_hash').fetch(1))
            while len(elems) == 0:
                time.sleep(self._PUT_SLEEP_TIME)
                elems = list(db.Query(SetAllElemsPQ2).filter('q =', q).filter('p =', p).filter('set_id =', set_id).order('elem_hash').fetch(1))
                #raise MinHashModelException( '__minhash(self, set_id=%d, q=%d)  p=%d, elems= in SetAllElemsPQ2 not available' % (set_id,q,p) )
                #return None
            minhash += elems[0].elem_hash + self._ITEM_HASH_SEP
        return minhash[:-1]


    def join_integers(self, int1, int2):
        m = hashlib.md5()
        m.update(str(int1))
        m.update(str(int2))        
        m.digest()        
        return int(m.hexdigest()[:self._HASH_BIT_LENGTH/4],16)


    def sanitize_string(self, s):
        s = s.replace(self._USER_SEP,'')
        return s
    
    
    def get_top_sets(self, n=None):
        if not n:
            n = self._MAX_PER_PAGE
            
        sets = list(db.Query(SetCount).order('-count').fetch(n))
        return [(s.count,s.set_id) for s in sets]
    
    
    def get_top_sets_ident(self, n=None, client_id=None, bucket_id=None, randomized=False):
        if not n:
            n = self._MAX_PER_PAGE
            
        sets = list(db.Query(SetCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).order('-count').fetch(n))
        ret = [(s.count,s.set_id) for s in sets]

        if randomized:
            ret = self._choose_randomized(ret, n)
        
        return n and ret[:n] or ret
    
    
    def get_top_elem(self, n=None): 
        if not n:
            n = self._MAX_PER_PAGE
            
        elems = list(db.Query(ElemCount).order('-count').fetch(n))
        return [(s.count,s.elem_id) for s in elems]
    
    
    def get_top_elems_ident(self, n=None, client_id=None, bucket_id=None, randomized=False):
        if not n:
            n = self._MAX_PER_PAGE
            
        sets = list(db.Query(ElemCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).order('-count').fetch(n))
        ret = [(s.count,s.elem_id) for s in sets]
        
        if randomized:
            ret = self._choose_randomized(ret, n)
        
        return n and ret[:n] or ret
    
    
    def add_to_set_with_ident(self, setid, elemid, client_id, bucket_id):
        
        self.get_seeds()
        
        orig_setid = setid
        orig_elemid = elemid
        setid = self.pack_client_bucket(setid,client_id,bucket_id)
        elemid = self.pack_client_bucket(elemid,client_id,bucket_id)
        add_ok = self.add_to_set(setid, elemid)
        if add_ok:
            self._update_counts_ident(orig_setid, orig_elemid, client_id, bucket_id)
        
    
    def add_to_set(self, setid, elemid):
        '''
        Symmetric add for set-elem, set-set, elem-set, elem-elem queries.
         
        '''
        # prefix SE% for set-elem set-set queries.
        ret1 = self._add_to_set(self._SET_ELEM_PREFIX+setid, self._SET_ELEM_PREFIX+elemid)
        # prefix ES% for elem-set elem-elem queries.
        ret2 = self._add_to_set(self._ELEM_SET_PREFIX+elemid, self._ELEM_SET_PREFIX+setid)
        
        return ret1 and ret2


    def _update_counts_ident(self, setid, elemid, client_id, bucket_id):
        # set
        setcounts = list(db.Query(SetCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).filter('set_id =', setid).fetch(1))
        if len(setcounts) > 0:
            setcount = setcounts[0]
            setcount.count += 1
        else:
            setcount = SetCount(
                                set_id = setid,
                                count = 1,
                                client_id = client_id,
                                bucket_id = bucket_id  
                                )
        setcount.put()
        # elem
        elemcounts = list(db.Query(ElemCount).filter('client_id =', client_id).filter('bucket_id =', bucket_id).filter('elem_id =', elemid).fetch(1))
        if len(elemcounts) > 0:
            elemcount = elemcounts[0]
            elemcount.count += 1
        else:
            elemcount = ElemCount(
                                elem_id = elemid,
                                count = 1,
                                client_id = client_id,
                                bucket_id = bucket_id  
                                )
        elemcount.put()
        

    def _add_to_set(self, setid, elemid):
        # Reducing latency http://stackoverflow.com/questions/14415878/how-can-i-reduce-google-app-engine-datastore-latency
        '''
        Only assymetric add only for set-elem, set-set.
        
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
        setid = self.sanitize_string(setid)
        elemid = self.sanitize_string(elemid)
        
        # check if exists, avoid duplicates, early optimization? remove this for future multiset impl.
        allelems = list(db.Query(SetAllElems2).filter('set_id =', setid).filter('elem_id =', elemid).fetch(1))
        if len(allelems) > 0:
            return False
        
        # store event tuple
        SetAllElems2(
            set_id = setid,
            elem_id = elemid,
            ).put()

        # TODO: Parallelizable
        # update clusters
        
        # optimized for q aggregated queries.
        for q in range(self.__q_recall):

            self._add_to_set_q(setid, elemid, q)
        
        return True
    
    
    def _add_to_set_q(self, setid, elemid, q):
    
        # plain random hash for elem
        p_randomhashelem_list = self._random_hash_p_tuple(elemid,q)
        
        # retrieve old minhash and compute new one
        old_minhash_list = list(db.Query(SetMinHash2).filter('q_iteration =', q).filter('set_id =', setid).fetch(1))
        
        is_new_set = len(old_minhash_list) == 0            
        self.__store_set_allelems(setid, p_randomhashelem_list, q)
        time.sleep(self._PUT_SLEEP_TIME)
        new_minhash = self.__minhash(setid, q)
            
        # new set store                    
        if is_new_set:
            old_minhash = None
            minhash_record = SetMinHash2(
                                            q_iteration = q,
                                            set_id = setid,
                                            p_joined_min_elem_hashes = new_minhash
                                            )
        else:
            old_minhash = old_minhash_list[0].p_joined_min_elem_hashes            
            minhash_record = old_minhash_list[0]
            
        # new cluster
        new_joined_set_cluster_list = list(db.Query(MinHashSetCluster2).filter('q_iteration =', q).filter('p_joined_min_elem_hashes =', new_minhash).fetch(1))
        new_cluster = len(new_joined_set_cluster_list) == 0
        if new_cluster:
            new_joined_set_cluster = MinHashSetCluster2(
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
            old_joined_set_cluster = list(db.Query(MinHashSetCluster2).filter('q_iteration =', q).filter('p_joined_min_elem_hashes =', old_minhash).fetch(1))[0]
            old_joined_set_cluster.joined_set_cluster = self._remove_from_joined_cluster(old_joined_set_cluster.joined_set_cluster, setid)
            
            # notice: we don't delete empty clusters to avoid appengine delete() latency aprox x4 times put() latency 
            # http://code.google.com/status/appengine/detail/datastore/2014/06/17#ae-trust-detail-datastore-delete-latency
            if old_joined_set_cluster.joined_set_cluster == self._EMPTY_SET:
                old_joined_set_cluster.delete()
                self.delete_cache_minhashsetcluster(q, old_minhash)
            else:
                old_joined_set_cluster.put()      
                self.update_cache_minhashsetcluster(q, old_minhash, [old_joined_set_cluster])          
                    
        new_joined_set_cluster.put()
        minhash_record.put()
        # update cache
        self.update_cache_minhashsetcluster(q, new_minhash, [new_joined_set_cluster])
        self.update_cache_setminhash(q, setid, [minhash_record])                    

        














