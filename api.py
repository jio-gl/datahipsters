#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 20/11/2014

@author: jose
'''

import time
import json

import webapp2

import traceback

from google.appengine.runtime import DeadlineExceededError
from google.appengine.ext import deferred

#from google.appengine.ext import webapp
#from google.appengine.ext.webapp2.util import run_wsgi_app

#from test import *

from jinja2 import Template

from model import *
from configuration import DHConfiguration


class DHAPIException(Exception):
    pass

class DHAPIAuthException(DHAPIException):
    pass


class DataHipstersAPI(object):

    __ADMIN_SECRET = 'DgrvFDGYT75FGYGHfFTfRf874fjhrfgtfgg64467gfthu6ggfvdrv367HdFgFvFrFFT5248HdW64747373fhfh37fh4S'
    # https://datahipsters.appspot.com/admin_client?admin_secret=DgrvFDGYT75FGYGHfFTfRf874fjhrfgtfgg64467gfthu6ggfvdrv367HdFgFvFrFFT5248HdW64747373fhfh37fh4S

    items_default_widget = '''
        <p>
        {% for result in results %}
        <div><a href="http://yoururl.com/item/{{result.item_id}}">{{result.item_id}}</a></div>
        {% endfor %}        
        </p>
        '''
    users_default_widget = '''
        <p>
        {% for result in results %}
        <div><a href="http://yoururl.com/user/{{result.user_id}}">{{result.user_id}}</a></div>
        {% endfor %}        
        </p>
        '''



    def __init__(self):
        self.initial_time = time.time()
        
    
    def get_elapsed_time(self):
        return time.time() - self.initial_time
        


    def check_credentials_admin(self,requests):
        
        try:
            self.admin_secret = requests.get("admin_secret")
        except:
            raise DHAPIException('admin_secret parameter missing in request!')
        
        if self.admin_secret != self.__ADMIN_SECRET:
            raise DHAPIException('wrong admin_secret parameter in request!')
    
    
    def check_credentials_basic(self,requests):
        
        if requests.get("client_id") == '':
            raise DHAPIAuthException('client_id parameter missing in request.')
    
    
    def check_credentials_recos(self,client_id,reco_token):
        
        users = list(db.Query(DataHipstersUser2).filter('client_id =', client_id).fetch(1))
        if len(users) == 0:
            raise DHAPIAuthException('invalid client_id parameter "%s" in request.' % client_id)
        else:
            record = users[0]
            
            if record.reco_token != reco_token:
                raise DHAPIAuthException('invalid reco_token parameter "%s" in request.' % reco_token)
            elif record.recos_left == 0:
                raise DHAPIAuthException('quota limit for this kind of requests (similar list or recommendations) reached! (client level %d)' % record.user_level)
            else:
                record.recos_left -= 1
            
            record.put()
            
        self.client = record
            
            
    def check_credentials_actions(self,client_id,action_token):
        
        users = list(db.Query(DataHipstersUser2).filter('client_id =', client_id).fetch(1))
        if len(users) == 0:
            raise DHAPIAuthException('invalid client_id parameter "%s" in request.' % client_id)
        else:
            record = users[0]
            
            if record.action_token != action_token:
                raise DHAPIAuthException('invalid action_token parameter "%s" in request.' % action_token)
            elif record.actions_left == 0:
                raise DHAPIAuthException('quota limit for this kind of requests (register action) reached! (client level %d)' % record.user_level)
            else:
                record.actions_left -= 1
            
            record.put()
            
    
    
    
    def check_credentials_add_event(self,requests):
        
        try:
            self.add_token = requests.get("add_token")
        except:
            raise DHAPIException('add_token parameter missing in request.')


    def get_and_validate_n(self, reqhandler):
        n = reqhandler.request.get("n")
        if n == '' or n == None:
            n = DHConfiguration.N_DEFAULT_RESULTS
        try:
            n = int(n)
        except:
            n = DHConfiguration.N_DEFAULT_RESULTS
        if n > MinHash._MAX_PER_PAGE:
            n = MinHash._MAX_PER_PAGE
        return n


    def get_and_validate_id(self, reqhandler, name=None, required=True):
        _id = reqhandler.request.get(name)
        if (_id == '' or _id == None) and name == 'bucket_id':
            _id = '0'
        #elif (_id == '' or _id == None) and name == 'client_id':
        #    _id = '0002'
        elif (_id == '' or _id == None) and required:
            raise DHAPIException('parameter "%s" is null!' % name)
        _id = _id[:MinHash._MAX_STRING_LEN]
        return _id


    def get_and_validate_boolean(self, reqhandler, name=None, required=True, default=True):
        '''
        Default value is True if not required and missing.
        '''
        _id = reqhandler.request.get(name)
        if (_id == '' or _id == None) and not required:
            _id = default
        elif _id == '' or _id == None and required:
            raise DHAPIException('parameter "%s" is null!' % name)
        else:
            _id = _id.lower() in ['true','yes','on'] and True or False
        return _id


    def build_minhash(self):
        self.p = DHConfiguration.P_HASH_CARDINALITY # for basic tests  # range 2 to 4
        self.q = DHConfiguration.Q_HASH_SAMPLES # for basic tests # range 10 to 20
        return MinHash(p=self.p,q=self.q)


    def stub(self, handler):

        extra_msg = ' // If this message is not informative, please let\'s us know (hello@datahipsters.com).'

        try:
            # generic checks
            
            self.widget = False
            self.image = False
            self.client_id = None
            
            # generic active objects            
            handler_method = handler.get_method()
            
            # calling specific method to handle request
            # set widget boolean and client_id if widget is true.
            try:
                ret,dump = handler_method()
            except DeadlineExceededError, e:
                ret, dump = {},''

            if not self.widget:
                # generic response construction
                ret['time'] = float('%.3f' % handler.api.get_elapsed_time())
                ret['status'] ='Ok'    
                handler.response.headers['Content-Type'] = 'application/json'
                handler.response.out.write( json.dumps(ret,indent=4, separators=(',', ': ')) )

                if DHConfiguration.DEBUG:
                    handler.response.out.write('\n\nTesting: \n\n%s' % (dump) )
            
            else: # is a widget, using Jinja2 templating
                # 0) build object list, read Jinja2 docs
                if not 'results' in ret:
                    results = []
                else:
                    results = ret['results']
                # 1) retrieve widget template
                template = self.widget_template
                # 2) fill template with details
                if template != '':
                    template = Template(template)
                    rendered = template.render(results=results)
                else:
                    rendered = ''
                #handler.response.headers['Content-Type'] = 'text/html'
                #handler.response.out.write( rendered )
                
                jsonp_id = handler.request.get('callback')
                
                ret = {'html': rendered }
                ret['time'] = float('%.3f' % handler.api.get_elapsed_time())
                ret['status'] ='Ok'    

                handler.response.headers['Content-Type'] = 'text/javascript'
                handler.response.out.write( ('%s ( '%jsonp_id) + json.dumps(ret,indent=4, separators=(',', ': ')) + ' )' )
                #handler.response.out.write( '? ( ' + json.dumps(ret,indent=4, separators=(',', ': ')) + ' )' )                
                #handler.response.out.write( rendered )
            
        except DHAPIAuthException, e:

            ret = {}
            handler.response.clear()
            ret['status'] ='Forbidden 403.'
            ret['message'] = str(e) + extra_msg
            handler.response.headers['Content-Type'] = 'application/json'
            handler.response.out.write( json.dumps(ret,indent=4, separators=(',', ': ')) )  
            handler.response.status = 403 
            
        except DHAPIException, e:

            ret = {}
            handler.response.clear()
            ret['status'] ='Bad request 400.'
            ret['message'] = str(e) + extra_msg
            handler.response.headers['Content-Type'] = 'application/json'
            handler.response.out.write( json.dumps(ret,indent=4, separators=(',', ': ')) )  
            handler.response.status = 400 

        except Exception, e:
            
            #raise e

            ret = {}
            handler.response.clear()
            ret['status'] ='Server error 500.'
            ret['message'] = str(e) + extra_msg
            if DHConfiguration.DEBUG:
                ret['message'] += traceback.format_exc()
            handler.response.headers['Content-Type'] = 'application/json'
            handler.response.out.write( json.dumps(ret,indent=4, separators=(',', ': ')) )  
            handler.response.status = 200 #500 
        


def dump_models():
        rows = []
#        rows += list(PermutationSeed.all())        
#        rows += list(SetAllElems2.all())
#        rows += list(SetAllElemsPQ2.all())
#        rows += list(MinHashSetCluster2.all())
#        rows += list(SetMinHash2.all())

        dump = ''        
        
        rows = list(PermutationSeed.all())
        dump += 'PermutationSeed\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            
        
        rows = list(SetAllElems2.all())
        dump += 'SetAllElems2\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            
        
        rows = list(SetAllElemsPQ2.all())
        dump += 'SetAllElemsPQ2\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            
        
        rows = list(MinHashSetCluster2.all())
        dump += 'MinHashSetCluster2\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            
        
        rows = list(SetMinHash2.all())
        dump += 'SetMinHash2\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            

        rows = list(SetCount.all())
        dump += 'SetCount\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            

        rows = list(ElemCount.all())
        dump += 'ElemCount\n'
        for r in rows:        
            dump += str(r) + "\n" 
        dump += '\n'            

        return dump


def debug_dump(rows=None):

    if DHConfiguration.DEBUG:
        # DEBUG
        dump = ''
        if rows:
            for s in rows:
                dump += str(s) + '\n'
            
        dump += '\n'
        dump += dump_models()
        # end DEBUG
    else:
        dump = ''

    return dump
