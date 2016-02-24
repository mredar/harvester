import os
from unittest import TestCase
import json
from datetime import datetime as DT
from mock import patch
from test.utils import DIR_FIXTURES
from harvester.solr_updater import main as solr_updater_main
from harvester.solr_updater import push_doc_to_solr, map_couch_to_solr_doc
from harvester.solr_updater import OldCollectionException
from harvester.solr_updater import CouchdbLastSeq_S3
from harvester.solr_updater import get_key_for_env
from harvester.solr_updater import has_required_fields
from harvester.solr_updater import get_solr_id
from harvester.solr_updater import normalize_sort_field
from harvester.solr_updater import get_sort_collection_data_string
from harvester.solr_updater import map_registry_data
from harvester.solr_updater import UTC
from harvester.solr_updater import dejson

class SolrUpdaterTestCase(TestCase):
    '''Test the solr update from couchdb changes feed'''
    def setUp(self):
        self.old_data_branch = os.environ.get('DATA_BRANCH', None)
        os.environ['DATA_BRANCH'] = 'test_branch'

    def tearDown(self):
        if self.old_data_branch:
            os.environ['DATA_BRANCH'] = self.old_data_branch
        
    @patch('solr.Solr', autospec=True)
    def test_push_doc_to_solr(self, mock_solr):
        '''Unit test calls to solr'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        push_doc_to_solr(sdoc, mock_solr)
#        mock_solr.add.assert_called_with({'repository_name': [u'Bancroft Library'], 'url_item': u'http://ark.cdlib.org/ark:/13030/ft009nb05r', 'repository': [u'https://registry.cdlib.org/api/v1/repository/4/'], 'publisher': u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/', 'collection_name': [u'Uchida (Yoshiko) photograph collection'], 'format': u'mods', 'rights': [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'], 'collection': [u'https://registry.cdlib.org/api/v1/collection/23066/'], 'id': u'23066--http://ark.cdlib.org/ark:/13030/ft009nb05r', 'campus_name': [u'UC Berkeley'], 'reference_image_md5': u'f2610262f487f013fb96149f98990fb0', 'relation': [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'], 'title': u'Neighbor', 'identifier': [u'http://ark.cdlib.org/ark:/13030/ft009nb05r', u'Banc Pic 1986.059:124--PIC'], 'type': u'image', 'campus': [u'https://registry.cdlib.org/api/v1/campus/1/'], 'subject': [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive']})

    def test_map_couch_to_solr_no_campus(self):
        doc = json.load(
                open(DIR_FIXTURES+'/couchdb_nocampus.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertNotIn('campus', sdoc)
        self.assertNotIn('campus_url', sdoc)
        self.assertNotIn('campus_name', sdoc)
        self.assertNotIn('campus_data', sdoc)
        repo_data = ['https://registry.cdlib.org/api/v1/repository/4/::'
                     'Bancroft Library']
        self.assertEqual(sdoc['repository_data'], repo_data)
        self.assertEqual(sdoc['sort_title'],
                u'neighbor my neighbor what a happy boy')

    def test_normalize_sort_field(self):
        self.assertEqual(normalize_sort_field('XXXXX'), 'xxxxx')
        self.assertEqual(normalize_sort_field('The XXXXX'), 'xxxxx')
        self.assertEqual(normalize_sort_field('XXXXX The'), 'xxxxx the')
        self.assertEqual(normalize_sort_field('A XXXXX'), 'xxxxx')
        self.assertEqual(normalize_sort_field('XXXXX A'), 'xxxxx a')
        self.assertEqual(normalize_sort_field('An XXXXX'), 'xxxxx')
        self.assertEqual(normalize_sort_field('XXXXX An'), 'xxxxx an')
        t_punc = '"This_ Title! has .punctuation$%%%)9 09qetk: YEAH!!'
        self.assertEqual(normalize_sort_field(t_punc),
                        'this title has punctuation9 09qetk yeah')

    def test_map_date_not_a_list(self):
        '''Test how the mapping works when the sourceResource/date is a dict
        not a list
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_solr_date_map.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['date'], ['between 1885-1890'])

    def test_map_couch_to_solr_nuxeo_doc(self):
        '''Test the mapping of a couch db source json doc from Nuxeo
        to a solr schema compatible doc
        '''
        #doc = json.load(open(DIR_FIXTURES+'/nuxeo_couchdb_doc.json'))
        doc = json.load(open(DIR_FIXTURES+'/26098--0025ad8f-a44e-4f58-8238-c7b60b2fb850.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], '0025ad8f-a44e-4f58-8238-c7b60b2fb850')
        self.assertEqual(sdoc['harvest_id_s'], '26098--0025ad8f-a44e-4f58-8238-c7b60b2fb850')
        self.assertEqual(sdoc['title'], ['Brag'])
        self.assertEqual(sdoc['sort_title'], 'brag')
        self.assertEqual(sdoc['alternative_title'], ['test alt title'])
        self.assertEqual(sdoc['creator'], ['Dunya Ramicova'])
        self.assertEqual(sdoc['description'], [u'"Design #39; BRAG; Entourage; Principle Base A" written on drawing. No signature on drawing.',
            u'Director: David Pountney; Scene Designer: Robert Israel; Producer: English National Opera, London, UK',
            u'English National Opera'])
        self.assertEqual(sdoc['reference_image_dimensions'], u'899:1199')
        self.assertEqual(sdoc['extent'], u'9" x 12" ')
        self.assertEqual(sdoc['format'],
            u'Graphite pencil, and Dr. Ph Martins Liquid Watercolor on watercolor paper')
        self.assertEqual(sdoc['genre'], ['Drawing'])
        self.assertNotIn('identifier', sdoc)        
        self.assertEqual(sdoc['language'], ['English', 'eng'])
        self.assertEqual(sdoc['provenance'], [u'Gift of Dunya Ramicova, 2014'])
        self.assertNotIn('publisher', sdoc)
        self.assertEqual(sdoc['relation'], [u'The Fairy Queen'])
        self.assertEqual(sdoc['rights'], 
                [u'copyrighted',
                u'Creative Commons Attribution - NonCommercial-NoDerivatives (CC BY-NC-ND 4.0)'])
        self.assertEqual(sdoc['structmap_text'], 'Brag')
        self.assertEqual(sdoc['structmap_url'],
                u's3://static.ucldc.cdlib.org/media_json/0025ad8f-a44e-4f58-8238-c7b60b2fb850-media.json')
        self.assertEqual(sdoc['subject'], [None])
        self.assertEqual(sdoc['type'], 'image')
 
    def test_map_couch_to_solr_doc(self):
        '''Test the mapping of a couch db source json doc to a solr schema
        compatible doc.
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], 'ark:/13030/ft009nb05r')
        self.assertEqual(sdoc['harvest_id_s'], '23066--http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertNotIn('campus', sdoc)
        self.assertEqual(sdoc['campus_url'], [u'https://registry.cdlib.org/api/v1/campus/1/'])
        self.assertEqual(sdoc['campus_name'], [u'UC Berkeley'])
        self.assertEqual(sdoc['campus_data'],
                [u'https://registry.cdlib.org/api/v1/campus/1/::UC Berkeley'])
        self.assertNotIn('repository', sdoc)
        self.assertEqual(sdoc['repository_url'],
                         [u'https://registry.cdlib.org/api/v1/repository/4/'])
        self.assertEqual(sdoc['repository_name'], [u'Bancroft Library'])
        repo_data = ['https://registry.cdlib.org/api/v1/repository/4/::'
                     'Bancroft Library::UC Berkeley']
        self.assertEqual(sdoc['repository_data'], repo_data)
        self.assertNotIn('collection', sdoc)
        self.assertEqual(sdoc['collection_url'],
                         ['https://registry.cdlib.org/api/v1/collection/23066/'])
        self.assertEqual(sdoc['collection_name'],
                         ['Uchida (Yoshiko) photograph collection'])
        c_data = ['https://registry.cdlib.org/api/v1/collection/23066/::'
                  'Uchida (Yoshiko) photograph collection']
        self.assertEqual(sdoc['collection_data'], c_data)
        self.assertEqual(sdoc['url_item'], u'http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertEqual(sdoc['contributor'], ['contrib 1', 'contrib 2'])
        self.assertEqual(sdoc['coverage'], ['Palm Springs (Calif.)', 'San Jacinto Mountains (Calif.)', 'Tahquitz Stream', 'Tahquitz Canyon'])
        self.assertEqual(sdoc['creator'], [u'creator 1', u'creator 2'])
        self.assertEqual(sdoc['description'], [u'description 1',
                                        u'description 2', u'description 3'])
        self.assertEqual(sdoc['date'], ['between 1885-1890'])
        self.assertEqual(sdoc['language'], ['English'])
        self.assertEqual(sdoc['publisher'], [u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/']),
        self.assertEqual(sdoc['relation'], [
            u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc',
            u'http://bancroft.berkeley.edu/collections/jarda.html',
            u'hb158005k9',
            u'BANC PIC 1986.059--PIC',
            u'http://calisphere.universityofcalifornia.edu/',
            u'http://bancroft.berkeley.edu/'])
        self.assertEqual(sdoc['rights'], [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'])
        self.assertEqual(sdoc['subject'], [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive'])
        self.assertEqual(sdoc['title'], [u'Neighbor'])
        self.assertEqual(sdoc['sort_title'], u'neighbor')
        self.assertEqual(sdoc['type'], u'image')
        self.assertEqual(sdoc['format'], 'mods')
        self.assertTrue('extent' not in sdoc)
        self.assertEqual(sdoc['sort_title'], u'neighbor')

    def test_sort_title_all_punctuation(self):
        doc = json.load(open(DIR_FIXTURES+'/couchdb_title_all_punc.json'))
        doc['sourceResource']['title'] = ['????$$%(@*#_!']
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], u'0025ad8f-a44e-4f58-8238-c7b60b2fb850')
        self.assertEqual(sdoc['sort_title'], '~title unknown')

    def test_sort_collection_data_string(self):
        '''
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        collection = doc['originalRecord']['collection'][0]
        sort_data = get_sort_collection_data_string(collection)
        self.assertEqual(sort_data,
                ':'.join(("uchida yoshiko photograph collection",
                  "Uchida (Yoshiko) photograph collection",
                  "https://registry.cdlib.org/api/v1/collection/23066/")))

    def test_map_registry_data(self):
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        collections = doc['originalRecord']['collection']
        reg_data = map_registry_data(collections)
        self.assertEqual(reg_data['sort_collection_data'],
                [u'uchida yoshiko photograph collection:Uchida (Yoshiko) photograph collection:https://registry.cdlib.org/api/v1/collection/23066/'])

    def test_decade_facet(self):
        '''Test generation of decade facet
        Currently generated from sourceResource.date.displayDate
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['facet_decade'], set(['1880s','1890s']))
        # no "date" in sourceResource
        doc = json.load(open(DIR_FIXTURES+'/couchdb_nocampus.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['facet_decade'], set([]))

    @patch('boto.s3.connect_to_region', autospec=True)
    def test_set_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq setting'''
        self.seq_obj = CouchdbLastSeq_S3()
        self.seq_obj.last_seq = 5
        mock_boto.assert_called_with('us-west-2')
        mock_boto('us-west-2').get_bucket.assert_called_with('solr.ucldc')
        mock_boto('us-west-2').get_bucket().get_key.assert_called_with('couchdb_since/test_branch')
        mock_boto('us-west-2').get_bucket().get_key().set_contents_from_string.assert_called_with(5)

    @patch('boto.s3.connect_to_region', autospec=True)
    def test_get_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq getting'''
        self.seq_obj = CouchdbLastSeq_S3()
        x = self.seq_obj.last_seq
        mock_boto.assert_called_with('us-west-2')
        mock_boto('us-west-2').get_bucket.assert_called_with('solr.ucldc')
        mock_boto('us-west-2').get_bucket().get_key.assert_called_with('couchdb_since/test_branch')
        mock_boto('us-west-2').get_bucket().get_key().get_contents_as_string.assert_called_with()

    def test_old_collection(self):
        doc = json.load(open(DIR_FIXTURES+'/couchdb_norepo.json'))
        self.assertRaises(OldCollectionException, map_couch_to_solr_doc, doc )

    def test_get_key_for_env(self):
        '''test correct key for env'''
        self.assertEqual(get_key_for_env(), 'couchdb_since/test_branch')

    def test_check_required_fields(self):
        doc = {'id':'test-id'}
        self.assertRaises(KeyError, has_required_fields, doc)
        doc = {'id':'test-id', 'sourceResource':{}}
        self.assertRaises(KeyError, has_required_fields, doc)
        doc = {'id':'test-id', 'sourceResource':{'title':'test-title'}}
        ret = has_required_fields(doc)
        self.assertEqual(ret, True)
        doc = {'id':'test-id', 'sourceResource':{'title':'test-title',
                                                'type': 'image'}}
        self.assertRaises(KeyError, has_required_fields, doc)
        doc = {'id':'test-id', 'object':'hasobject',
                'sourceResource':{'title':'test-title',
                                  'type': 'image'}
        }
        ret = has_required_fields(doc)
        self.assertEqual(ret, True)

    def test_solr_pretty_id(self):
        '''Test the new solr id scheme on the various document types.
        see : https://github.com/ucldc/ucldc-docs/wiki/pretty_id
        arks are always pulled if found.
        Some institutions have known ark framents, arks are constructed
        for these.
        Nuxeo objects retain their UUID
        All other objects the harvest_id_s _id is md5sum
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_oac.json'))
        sid = get_solr_id(doc)
        self.assertEqual(sid, "ark:/13030/ft029002qb")
        doc = json.load(open(DIR_FIXTURES+'/couchdb_nuxeo.json'))
        sid = get_solr_id(doc)
        self.assertEqual(sid, "002c0501-26c6-4377-a0aa-5b30038c6edf")
        doc = json.load(open(DIR_FIXTURES+'/couchdb_ucsd.json'))
        sid = get_solr_id(doc)
        self.assertEqual(sid, "ark:/20775/bb0308012n")
        doc = json.load(open(DIR_FIXTURES+'/couchdb_no_pretty_id.json'))
        sid = get_solr_id(doc)
        #sha256 self.assertEqual(sid, '0b36b5bb2183de9c81577224d3964d120f911f2e44647319a0f62ffcbab77f6a')
        self.assertEqual(sid, '22a5713851ea0aca428adcf3caf4970b')
        doc = json.load(open(DIR_FIXTURES+'/couchdb_ucla.json'))
        sid = get_solr_id(doc)
        self.assertEqual(sid, "ark:/21198/zz002b1833")

    def test_sort_dates(self):
        '''test the sort_date_start/end values'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['sort_date_start'], DT(1885, 1, 1, tzinfo=UTC))
        self.assertEqual(sdoc['sort_date_end'], DT(1890, 1, 1, tzinfo=UTC))
        doc = json.load(open(DIR_FIXTURES+'/couchdb_no_pretty_id.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['sort_date_start'], DT(2013, 9, 30, tzinfo=UTC))
        self.assertEqual(sdoc['sort_date_end'], DT(2013, 9, 30, tzinfo=UTC))
        doc = json.load(open(DIR_FIXTURES+'/couchdb_nocampus.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertNotIn('sort_date_start', sdoc)
        self.assertNotIn('sort_date_end', sdoc)
        doc = json.load(open(DIR_FIXTURES+'/couchdb_solr_date_map.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['sort_date_start'], DT(1885, 7, 4, tzinfo=UTC))
        self.assertEqual(sdoc['sort_date_end'], DT(1890, 8, 3, tzinfo=UTC))

    def test_dejson(self):
        '''Test dejson directly'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_dejson_type_error.json'))
        dj = dejson('identifier', doc['sourceResource']['identifier'])
        self.assertEqual(dj, [u'piercephoto:10', u'uclamss_98pierce_0086_c0023',
            u'86'])
        doc = json.load(open(DIR_FIXTURES+'/couchdb_ucla.json'))
        dj = dejson('coverage', doc['sourceResource']['spatial'])
        self.assertEqual(dj, [ "Topanga (Calif.)",
                               "Pacific Palisades, Los Angeles (Calif.)",
                               "Venice (Los Angeles, Calif.)",
                               "Los Angeles (Calif.)" ])
        
    def test_dejson_from_map(self):
        '''Test that the dejson works from the mapping function'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_ucla.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['coverage'], [ "Topanga (Calif.)",
                               "Pacific Palisades, Los Angeles (Calif.)",
                               "Venice (Los Angeles, Calif.)",
                               "Los Angeles (Calif.)" ])
