# -*- coding: utf-8 -*-
# harvest source content
# This is not the metadata feed but the digital representaion
# of the object (or the digital object itself for born digital)


import os
import sys
import datetime
import time
import urlparse
from couchdb import ResourceConflict
import requests
import md5s3stash
import boto.s3
import logging
from collections import namedtuple
from collections import defaultdict
from harvester.couchdb_init import get_couchdb
from harvester.config import config
from redis import Redis
import redis_collections
from harvester.couchdb_pager import couchdb_pager
from harvester.cleanup_dir import cleanup_work_dir
from harvester.sns_message import publish_to_harvesting
from harvester.sns_message import format_results_subject

# save to multiple? to avoid deletion. Different regions...
BUCKET_BASES = os.environ.get(
    'S3_BUCKET_IMAGE_BASE',
    'us-west-2:static-ucldc-cdlib-org/harvested_images;us-east-1:'
    'static.ucldc.cdlib.org/harvested_images').split(';')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')

logging.basicConfig(level=logging.DEBUG, )

################################################################################
# Exceptions
################################################################################
class ContentHarvestError(Exception):
    def __init__(self, message, doc_id=None):
        super(ImageHarvestError, self).__init__(message)
        self.doc_id = doc_id


class ImageHTTPError(ContentHarvestError):
    # will waap exceptions from the HTTP request
    dict_key = 'HTTP Error'


class HasObject(ContentHarvestError):
    dict_key = 'Has Object already'


class RestoreFromObjectCache(ContentHarvestError):
    dict_key = 'Restored From Object Cache'


class IsShownByError(ContentHarvestError):
    dict_key = 'isShownBy Error'


class FailsImageTest(ContentHarvestError):
    dict_key = 'Fails the link is to image test'

################################################################################
# Exceptions - end
################################################################################


class ContentHandler(object):
    '''Abstract Handler
    Handle specific content. create and delete for each object?
    no can probably be static class & fn on it?

    In general, this saves data to S3, then updates the couchdb doc to reflect
    the results. Different content has different outcomes.

    See code for current handlers and content mapping

    '''
    def __init__(self):
        pass

    def process(doc, **kwargs):
        '''process the current couchdb doc
        sub classes implement'''
        pass

class ImageHandler(ContentHandler):
    '''Handle type of image'''
    pass

class ImageHandler(ContentHandler):
    '''Handle type of image'''
    pass

class ContentHandlerFactory(object):
    '''This figures out which content handler to create for a given
    doc.
    It is a singleton and holds references and values to pass to handlers?
    There is not really a good way to handle content from our various sources.
    Stuff is mislabeled, servers serve up various formats that may or may not
    correspond  to document's stated type
    '''
    def getHandler(self, doc):
        '''Return the correct handler for the given document.
        '''
        srcRes = doc['sourceResource']
        # sourceResource.type check
        #
        if srcRes['type'].lowercase() == 'image':
            return ImageHandler(doc)
        elif srcRes['type'].lowercase() == 'text':
            return TextHandler(doc)

class ContentHarvester(object):
    '''Useful to cache couchdb, authentication info and such'''

    def __init__(self,
                 cdb=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 bucket_bases=BUCKET_BASES,
                 object_auth=None,
                 get_if_object=False,
                 url_cache=None,
                 hash_cache=None,
                 harvested_object_cache=None):
        self._config = config()
        if cdb:
            self._couchdb = cdb
        else:
            if not url_couchdb:
                url_couchdb = self._config['couchdb_url']
            self._couchdb = get_couchdb(url=url_couchdb, dbname=couchdb_name)
        self._bucket_bases = bucket_bases
        self._view = couch_view
        # auth is a tuple of username, password
        self._auth = object_auth
        self.get_if_object = get_if_object  # if object field exists, get
        self._redis = Redis(
            host=self._config['redis_host'],
            port=self._config['redis_port'],
            password=self._config['redis_password'],
            socket_connect_timeout=self._config['redis_connect_timeout'])
        self._url_cache = url_cache if url_cache is not None else \
            redis_collections.Dict(key='ucldc-image-url-cache',
                                   redis=self._redis)
        self._hash_cache = hash_cache if hash_cache is not None else \
            redis_collections.Dict(key='ucldc-image-hash-cache',
                                   redis=self._redis)
        self._object_cache = harvested_object_cache if harvested_object_cache \
            else \
            redis_collections.Dict(
                key='ucldc:harvester:harvested-images',
                redis=self._redis)


    def harvest_content(self, doc):
        '''harvest the content for the given CouchDB document'''
        pass

# Copyright © 2017, Regents of the University of California
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# - Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of the University of California nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
