import unittest
import time
import json
import os

import webapp2

from google.appengine.ext import deferred
from google.appengine.api import memcache

#from google.appengine.ext import webapp
#from google.appengine.ext.webapp2.util import run_wsgi_app

#from test import *

from model import *
from configuration import DHConfiguration
from api import DataHipstersAPI,dump_models,debug_dump

from widget import widget_template

# Payments with Paypal:  
# http://www.supernifty.org/articles/oreilly-gae-adaptive-market/gae-adaptive.htm
# http://www.supernifty.org/blog/2011/02/28/google-app-engine-paypal-python-tutorial/
# Stripe has a Python library to accept credit cards without needing a merchant account: https://github.com/stripe/stripe-python
# You can avoid PCI audits if the credit card details never touch your server... for example by using payment forms hosted on the servers of your chosen payment gateway provider.


class UserOrItemCache(object):
    
    def __init__(self, client_id, bucket_id, user_or_item_id):
        self.key1 = '%s:%s:%s' % (client_id, bucket_id, user_or_item_id)

    def get_if_cached(self, query_string_key):
        if not DHConfiguration.USE_URL_MEMCACHE:
            return None
        user_or_item_dict = memcache.get(self.key1)
        if user_or_item_dict is not None and query_string_key in user_or_item_dict:
            return user_or_item_dict[query_string_key]
        else:
            return None

    def save_to_cache(self, query_string_key, obj):
        if not DHConfiguration.USE_URL_MEMCACHE:
            return
        user_or_item_dict = memcache.get(self.key1)    
        if user_or_item_dict is None:
            user_or_item_dict = {query_string_key : obj}
        else:
            user_or_item_dict[query_string_key] = obj
        memcache.set(self.key1, user_or_item_dict, DHConfiguration.URL_CACHE_LIFE)

    def flush_cache(self):
        if not DHConfiguration.USE_URL_MEMCACHE:
            return
        user_or_item_dict = memcache.delete(self.key1)    
        

class MainPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.main_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def main_closure(self):
        
        #suite = unittest.TestLoader().loadTestsFromTestCase(TestMinHash)
        #unittest.TextTestRunner(verbosity=2).run(suite)
        
        dump = debug_dump()

        return {},dump


class AdminClientPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.admin_client_closure

    def post(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def admin_client_closure(self):

        ret = {}
         
        self.api.check_credentials_admin(self.request)        
        self.api.check_credentials_basic(self.request)
        
        params = [
                  "client_id",
                  "action_token",
                  "reco_token",
                  "actions_left",
                  "recos_left",
                  "user_level",
                  "widget_recommended_items",
                  "widget_recommend_item_to",
                  "widget_similar_items",
                  "widget_similar_users",
                  "widget_top_items",
                  "widget_top_users",
                  ]
        param_dict = {}
        for param in params:                
            param_dict[param] = self.request.get(param)
            if param_dict[param] == '':
                del param_dict[param]
        
        users = list(db.Query(DataHipstersUser2).filter('client_id =', param_dict["client_id"]).fetch(1))
        if len(users) == 0:
            record = DataHipstersUser2(
                    client_id = param_dict["client_id"],
                    action_token = param_dict["action_token"],
                    reco_token = param_dict["reco_token"],
                    actions_left = int(param_dict["actions_left"]),
                    recos_left = int(param_dict["recos_left"]),
                    user_level = int(param_dict["user_level"]),
                    widget_recommended_items = "widget_recommended_items" in param_dict and param_dict["widget_recommended_items"] or DataHipstersAPI.items_default_widget,                    
                    widget_recommend_item_to = "widget_recommend_item_to" in param_dict and param_dict["widget_recommend_item_to"] or DataHipstersAPI.users_default_widget,
                    widget_similar_items = "widget_similar_items" in param_dict and param_dict["widget_similar_items"] or DataHipstersAPI.items_default_widget,
                    widget_similar_users = "widget_similar_users" in param_dict and param_dict["widget_similar_users"] or DataHipstersAPI.users_default_widget,
                    widget_top_items = "widget_top_items" in param_dict and param_dict["widget_top_items"] or DataHipstersAPI.items_default_widget,
                    widget_top_users = "widget_top_users" in param_dict and param_dict["widget_top_users"] or DataHipstersAPI.users_default_widget,
                                      )
        
        else:
            record = users[0]
            if 'action_token' in param_dict:
                record.action_token = param_dict['action_token']
            else:
                param_dict['action_token'] = record.action_token
        
            if 'reco_token' in param_dict:
                record.reco_token = param_dict['reco_token']
            else:
                param_dict['reco_token'] = record.reco_token
        
            if 'actions_left' in param_dict:
                record.actions_left = int(param_dict['actions_left'])
            else:
                param_dict['actions_left'] = record.actions_left
        
            if 'recos_left' in param_dict:
                record.recos_left = int(param_dict['recos_left'])
            else:
                param_dict['recos_left'] = record.recos_left
        
            if 'user_level' in param_dict:
                record.user_level = int(param_dict['user_level'])
            else:
                param_dict['user_level'] = record.user_level
        
            if 'widget_recommended_items' in param_dict:
                record.widget_recommended_items = param_dict['widget_recommended_items']
            else:
                param_dict['widget_recommended_items'] = record.widget_recommended_items
        
            if 'widget_recommend_item_to' in param_dict:
                record.widget_recommend_item_to = param_dict['widget_recommend_item_to']
            else:
                param_dict['widget_recommend_item_to'] = record.widget_recommend_item_to
        
            if 'widget_similar_items' in param_dict:
                record.widget_similar_items = param_dict['widget_similar_items']
            else:
                param_dict['widget_similar_items'] = record.widget_similar_items
        
            if 'widget_similar_users' in param_dict:
                record.widget_similar_users = param_dict['widget_similar_users']
            else:
                param_dict['widget_similar_users'] = record.widget_similar_users
        
            if 'widget_top_items' in param_dict:
                record.widget_top_items = param_dict['widget_top_items']
            else:
                param_dict['widget_top_items'] = record.widget_top_items
        
            if 'widget_top_users' in param_dict:
                record.widget_top_users = param_dict['widget_top_users']
            else:
                param_dict['widget_top_users'] = record.widget_top_users
        
        record.put()

        dump = debug_dump([])
        
        ret['user_params'] = param_dict

        #self.response.out.write( '\n\n' + str(record) )
        return ret,dump
        
        


class RegisterActionPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.register_action_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def register_action_closure(self):
    
        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        action_token = self.api.get_and_validate_id(self, name='action_token')
        self.api.check_credentials_actions(client_id,action_token)
    
        user_id = self.api.get_and_validate_id(self, name='user_id')
        item_id = self.api.get_and_validate_id(self, name='item_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)

        #self.minhash.get_seeds()
        
        if DHConfiguration.DEFER_ACTIONS: # to defer            
            deferred.defer(self.minhash.add_to_set_with_ident, user_id, item_id, client_id, bucket_id)            
        else: # or not to defer 
            self.minhash.add_to_set_with_ident(user_id, item_id, client_id, bucket_id)
        
        # use cache
        UserOrItemCache(client_id, bucket_id, 'U'+user_id).flush_cache()
        UserOrItemCache(client_id, bucket_id, 'I'+item_id).flush_cache()
        UserOrItemCache(client_id, bucket_id, 'I').flush_cache()
        UserOrItemCache(client_id, bucket_id, 'U').flush_cache()
        
        dump = debug_dump([])
        
        self.api.image = False
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = ''

        return ret,dump
    

class RemoveItemPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.remove_item_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def remove_item_closure(self):
    
        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        action_token = self.api.get_and_validate_id(self, name='action_token')
        self.api.check_credentials_actions(client_id,action_token)
    
        item_id = self.api.get_and_validate_id(self, name='item_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)

        #self.minhash.get_seeds()
        
        if DHConfiguration.DEFER_ACTIONS: # to defer            
            deferred.defer(self.minhash.remove_elem_symm_ident, item_id, client_id, bucket_id)            
        else: # or not to defer 
            self.minhash.remove_elem_symm_ident( item_id, client_id, bucket_id )
        
        dump = debug_dump([])

        # use cache
        UserOrItemCache(client_id, bucket_id, 'I'+item_id).flush_cache()
        UserOrItemCache(client_id, bucket_id, 'I').flush_cache()
        
        self.api.image = False
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = ''

        return ret,dump
    

class RemoveUserPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.remove_user_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def remove_user_closure(self):
    
        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        action_token = self.api.get_and_validate_id(self, name='action_token')
        self.api.check_credentials_actions(client_id,action_token)
    
        user_id = self.api.get_and_validate_id(self, name='user_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)

        #self.minhash.get_seeds()
        
        if DHConfiguration.DEFER_ACTIONS: # to defer            
            deferred.defer(self.minhash.remove_set_symm_ident, user_id, client_id, bucket_id)            
        else: # or not to defer 
            self.minhash.remove_set_symm_ident( user_id, client_id, bucket_id )

        # use cache
        UserOrItemCache(client_id, bucket_id, 'U'+user_id).flush_cache()
        UserOrItemCache(client_id, bucket_id, 'U').flush_cache()
        
        dump = debug_dump([])
        
        self.api.image = False
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = ''

        return ret,dump
    

class ResetPage(webapp2.RequestHandler):    

    def get_method(self):
        return self.reset_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def reset_closure(self):
        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_admin(self.request)

        self.minhash.reset(reset_seeds=True)

        dump = dump_models()
        
        return ret,dump


class SimilarUsersPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.similar_users_closure

    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def similar_users_closure(self):
        
        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        user_id = self.api.get_and_validate_id(self, name='user_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')        
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False)

        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'U'+user_id)
        query_string_key = self.request.path + '/' + self.request.query_string
        similar = cache.get_if_cached(query_string_key)
        if similar is None:
            try:        
                similar = self.minhash.get_n_neighbors_set_elem_ident(user_id, n=n, client_id=client_id, bucket_id=bucket_id, randomized=randomized)
            except NoUserDataYet, e:
                similar = self.minhash.get_top_sets_ident(n, client_id, bucket_id, randomized)
            cache.save_to_cache(query_string_key, similar)
                   
        rows = similar
        dump = debug_dump(rows)

        ret['results'] = [ {'rating':sim,'user_id':ident} for sim,ident in similar]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_similar_users
        
        return ret,dump
        
    
            

class SimilarItemsPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.similar_items_closure
    
    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def similar_items_closure(self):

        ret = {}
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        item_id = self.api.get_and_validate_id(self, name='item_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False)

        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'I'+item_id)
        query_string_key = self.request.path + '/' + self.request.query_string
        similar = cache.get_if_cached(query_string_key)
        if similar is None:
            try:      
                similar = self.minhash.get_n_neighbors_elem_set_ident(item_id, n=n, client_id=client_id, bucket_id=bucket_id, randomized=randomized)
            except NoUserDataYet, e:
                similar = self.minhash.get_top_elems_ident(n, client_id, bucket_id, randomized)            
            cache.save_to_cache(query_string_key, similar)
      
        rows = similar
        dump = debug_dump(rows)

        ret['results'] = [ {'rating':sim,'item_id':ident} for sim,ident in similar]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_similar_items

        return ret,dump


class RecommendedItemsPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.recommended_items_closure
    
    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def recommended_items_closure(self):

        ret = {}
        dump = ''
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        user_id = self.api.get_and_validate_id(self, name='user_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False)
        
        
        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'U'+user_id)
        query_string_key = self.request.path + '/' + self.request.query_string
        recos = cache.get_if_cached(query_string_key)
        if recos is None:
            try:
                recos = self.minhash.get_set_elem_recommendations_ident(user_id, n=n,neighbors_limit=None, threshold=None, concurrent=False, client_id=client_id, bucket_id=bucket_id, randomized=randomized)
            except NoUserDataYet, e:
                recos = self.minhash.get_top_elems_ident(n, client_id, bucket_id, randomized)
            cache.save_to_cache(query_string_key, recos)

        #dump += str( self.minhash.get_set_elem_recommendations(user_id+'_Jose_0', n=n,neighbors_limit=None, threshold=None, concurrent=False, randomized=randomized) )

        rows = recos
        dump += debug_dump(rows)

        ret['results'] = [ {'rating':sim,'item_id':ident} for sim,ident in recos]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_recommended_items

        return ret,dump
        


class RecommendedUsersPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.recommended_users_closure
    
    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def recommended_users_closure(self):
        
        ret = {}
        dump = ''
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        item_id = self.api.get_and_validate_id(self, name='item_id')
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False)
        
        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'I'+item_id)
        query_string_key = self.request.path + '/' + self.request.query_string
        recos = cache.get_if_cached(query_string_key)
        if recos is None:
            try:
                recos = self.minhash.get_elem_set_recommendations_ident(item_id, n=n,neighbors_limit=None, threshold=None, concurrent=False, client_id=client_id, bucket_id=bucket_id, randomized=randomized)
            except NoUserDataYet, e:
                recos = self.minhash.get_top_sets_ident(n, client_id, bucket_id, randomized)
            cache.save_to_cache(query_string_key, recos)

        rows = recos
        dump += debug_dump(rows)

        ret['results'] = [ {'rating':sim,'user_id':ident} for sim,ident in recos]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_recommend_item_to

        return ret,dump


class TopItemsPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.top_items_closure
    
    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def top_items_closure(self):
        
        ret = {}
        dump = ''
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False, default=False)
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'I')
        query_string_key = self.request.path + '/' + self.request.query_string
        recos = cache.get_if_cached(query_string_key)
        if recos is None:
            recos = self.minhash.get_top_elems_ident(n, client_id, bucket_id, randomized=randomized)
            cache.save_to_cache(query_string_key, recos)

        rows = recos
        dump += debug_dump(rows)

        ret['results'] = [ {'rating':sim,'item_id':ident} for sim,ident in recos]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_top_items

        return ret,dump


class TopUsersPage(webapp2.RequestHandler):    
    
    def get_method(self):
        return self.top_users_closure
    
    def get(self):      
        self.api = DataHipstersAPI()
        self.api.stub( self )
    
    def top_users_closure(self):
        
        ret = {}
        dump = ''
        
        self.minhash = self.api.build_minhash()
        
        self.api.check_credentials_basic(self.request)
        
        client_id = self.api.get_and_validate_id(self, name='client_id')
        reco_token = self.api.get_and_validate_id(self, name='reco_token')
        self.api.check_credentials_recos(client_id,reco_token)
    
        bucket_id = self.api.get_and_validate_id(self, name='bucket_id')
        n = self.api.get_and_validate_n(self) #self.request.get("n")
        
        widget = self.api.get_and_validate_boolean(self, name='widget', required=False, default=False)
        
        randomized = self.api.get_and_validate_boolean(self, name='randomized', required=False, default=False)
        
        # use cache        
        cache = UserOrItemCache(client_id, bucket_id, 'U')
        query_string_key = self.request.path + '/' + self.request.query_string
        recos = cache.get_if_cached(query_string_key)
        if recos is None:
            recos = self.minhash.get_top_sets_ident(n, client_id, bucket_id, randomized=randomized)
            cache.save_to_cache(query_string_key, recos)

        rows = recos
        dump += debug_dump(rows)

        ret['results'] = [ {'rating':sim,'user_id':ident} for sim,ident in recos]
        ret['randomized'] = str(randomized).lower()
        
        self.api.widget = widget
        self.api.client_id = client_id
        if widget:
            self.api.widget_template = self.api.client.widget_top_users

        return ret,dump


class WidgetJSPage(webapp2.RequestHandler):    

    widget_loader = "$('#'+container_id).html(data.html);"

    def get(self):      
        self.response.headers['Content-Type'] = 'text/javascript'
        self.response.out.write( widget_template.replace('{{WIDGET_LOADER}}',self.widget_loader).replace('{{MAIN_NAME}}','widgetMain').replace('{{PATH}}',self.request.path) )


class ActionJSPage(webapp2.RequestHandler):    

    widget_loader = ""

    def get(self):      
        self.response.headers['Content-Type'] = 'text/javascript'
        self.response.out.write( widget_template.replace('{{WIDGET_LOADER}}',self.widget_loader).replace('{{MAIN_NAME}}','actionMain').replace('{{PATH}}',self.request.path) )


# https://developers.google.com/appengine/docs/python/python25/migrate27
app = webapp2.WSGIApplication([
                                      ('/', MainPage),
                                      ('/register_action', RegisterActionPage),
                                      ('/remove_item', RemoveItemPage),
                                      ('/remove_user', RemoveUserPage),
                                      ('/reset', ResetPage),
                                      ('/similar_users', SimilarUsersPage),
                                      ('/similar_items', SimilarItemsPage),
                                      ('/recommended_items', RecommendedItemsPage),
                                      ('/recommend_item_to', RecommendedUsersPage),
                                      ('/admin_client', AdminClientPage),
                                      ('/top_items', TopItemsPage),
                                      ('/top_users', TopUsersPage),
                                      ('/widget.js', WidgetJSPage),
                                      ('/widget\d+.js', WidgetJSPage),
                                      ('/action.js', ActionJSPage),
                                      ('/action\d+.js', ActionJSPage),
                                      ], debug=DHConfiguration.DEBUG)


# https://www.mortardata.com/ 
# http://kamisama.me/2013/05/24/first-steps-with-predictionio-a-recommendation-server/
# http://www.smartinsights.com/conversion-optimisation/product-page-optimisation/web-personalization-software/
