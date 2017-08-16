#! /bin/env python
# -*- coding: utf-8 -*-

# We've had a couple of cases where the pre-prodution index has had a
# collection deleted for re-harvesting but the re-harvest has not been
# successful and we want to publish a new image.
# This script will take the documents from one solr index and push them to
# another solr index
import os
import argparse
import json
import requests

# to get rid of ssl key warning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

URL_SOLR_API='https://solr.calisphere.org/solr/'
URL_SOLR=None
URL_REGISTRY_API='https://registry.cdlib.org/api/v1/collection/'

def get_ids_for_collection(url_collection, url_solr=URL_SOLR_API,
        api_key=None):
    '''Return solr IDs for a given collection.'''
    solr_auth = { 'X-Authentication-Token': api_key } if api_key else None
    query = { 'q': 'collection_url:{}'.format(url_collection), 'rows':100000,
            'fl': 'id'}
    solr_endpoint = url_solr + 'query'
    print "Getting ids from : {}\n{}".format(solr_endpoint, query)
    resp_obj =  requests.get(solr_endpoint,
                                    headers=solr_auth,
                                    params=query,
                                    verify=False)
    print resp_obj
    return [ d['id'] for d in resp_obj.json()['response']['docs']]

def get_solr_doc(sid, url_solr, api_key):
    solr_auth = { 'X-Authentication-Token': api_key } if api_key else None
    query = { 'q': 'id:"{}"'.format(sid) }
    resp_obj =  json.loads(requests.get(url_solr+'query',
                                    headers=solr_auth,
                                    params=query,
                                    verify=False).content)
    # need to filter out _version_
    doc =  resp_obj['response']['docs'][0]
    del doc['_version_']
    for k in doc.keys():
        if '_ss' in k:
            del doc[k]
    return doc

def add_doc(doc, solr_endpoint):
    resp = requests.post(solr_endpoint,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(doc),
            verify=False)
    if resp.status_code == 200:
        print 'synced {}'.format(doc['id'])
    else:
        print 'failed {} : {}'.format(doc['id'], resp.status_code)
    return True if resp.status_code == 200 else False

def get_update_endpoint(dest_solr):
    return dest_solr + '/update/json/docs'

def sync_id_list(ids, source_solr=None, update_endpoint=None,
        source_api_key=None):
    n_success = n_failed = 0
    for sid in ids:
        source_doc = get_solr_doc(sid, url_solr=source_solr,
                api_key=source_api_key)
        added = add_doc(source_doc, update_endpoint)
        if added:
            n_success += 1
        else:
            n_failed += 1
    # commit to index
    requests.get(dest_solr+'/update?commit=true')
    return n_success, n_failed

def sync_collection(url_collection, source_solr=None, update_endpoint=None,
        source_api_key=None):#, dest_api_key=None):
    ids = get_ids_for_collection(url_collection, source_solr, source_api_key)
    print "Syncing {} records".format(len(ids))
    return sync_id_list(
        ids,
        source_solr=source_solr,
        update_endpoint=update_endpoint,
        source_api_key=source_api_key)


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('collection_id')
    parser.add_argument('-y', action='store_true',
        help="-y to answer yes to input")

    argv = parser.parse_args()
    print "Y? {}".format(argv.y)
    url_collection = URL_REGISTRY_API + argv.collection_id + '/'
    source_solr = os.environ.get('URL_SOLR_API', URL_SOLR_API)
    source_api_key = os.environ.get('SOLR_API_KEY', '')
    dest_solr = os.environ.get('URL_SOLR', None)
    print "CID:{} SOLR SOURCE: {}  SOLR DESTINATION: {}".format(
        argv.collection_id,
        source_solr,
        dest_solr)
    solr_endpoint = get_update_endpoint(dest_solr)
    print "DESTINANTION ENDPOINT: {}".format(solr_endpoint)
    if not argv.y:
        raw_input('Press any key to continue')
    success, failed = sync_collection(url_collection, source_solr=source_solr,
            update_endpoint=solr_endpoint,
            source_api_key=source_api_key)
    print "{} updated successfully. {} failed".format(success, failed)


# Copyright © 2016, Regents of the University of California
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

