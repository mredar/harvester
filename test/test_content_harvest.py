# -*- coding: utf-8 -*-
import os
from unittest import TestCase
from collections import namedtuple
from mock import patch
from mock import MagicMock
from mypretty import httpretty
# import httpretty
from harvester import content_harvest
from harvester.content_harvest import FailsImageTest
from harvester.content_harvest import ImageHTTPError
from harvester.content_harvest import IsShownByError
from harvester.content_harvest import HasObject
from harvester.content_harvest import RestoreFromObjectCache

#TODO: make this importable from md5s3stash
StashReport = namedtuple('StashReport',
                         'url, md5, s3_url, mime_type, dimensions')



class ContentHarvestTestCase(TestCase):
    def setUp(self):
        self.old_url_couchdb = os.environ.get('COUCHDB_URL', None)
        os.environ['COUCHDB_URL'] = 'http://example.edu/test'

    def tearDown(self):
        if self.old_url_couchdb:
            os.environ['COUCHDB_URL'] = self.old_url_couchdb

    @patch('couchdb.Server')
    def test_class(self, mock_couchdb):
        ch = content_harvest.ContentHarvester()


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
