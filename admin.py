#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import collections
import csv
import json

import webapp2
from google.appengine.api import app_identity
from google.appengine.ext import deferred
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from apiclient import discovery
import cloudstorage as gcs
import httplib2
from oauth2client import appengine


SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_ID = app_identity.get_application_id()


class LoadHandler(webapp2.RequestHandler):
  def get(self):

    files = [x.filename for x in gcs.listbucket('/bqpipeline/raw-wikipedia/2014-07/')]
    result = {'files': []}
    for filename in files:
      result['files'].append(filename)
      tablename = filename.split("/")[4][:22].replace('-', '_')
      deferred.defer(
        load_table,
        'wikipedia_raw_201407',
        tablename,
        'gs:/%s' % filename)
    self.response.out.write(json.dumps(result))


class UnionQueryHandler(webapp2.RequestHandler):
  def get(self):
    table_list = bq_service().tables().list(projectId=PROJECT_ID, datasetId='wikipedia_raw_201407', maxResults=1000).execute()
    tables = ([x['id'] for x in table_list['tables']])
    selects = ['(SELECT TIMESTAMP("%s:00:00") datehour, * FROM [%s])' % (x[-11:].replace('_', ' '), x) for x in tables]
    union_select = 'SELECT * FROM %s LIMIT 10' % ','.join(selects)
    self.response.out.write(len(tables))
    self.response.out.write(collections.Counter([x[-11:-3] for x in tables]).most_common())
    self.response.out.write('\n')
    self.response.out.write(union_select)

  
app = webapp2.WSGIApplication([
    ('/admin/load', LoadHandler),
    ('/admin/unionQuery', UnionQueryHandler),
], debug=True)


def bq_service():
  credentials = appengine.AppAssertionCredentials(scope=SCOPE)
  http = credentials.authorize(httplib2.Http())
  return discovery.build("bigquery", "v2", http=http)


def load_table(dataset, target, source):
  service = bq_service()
  job_data = {
    'jobReference': {
      'projectId': PROJECT_ID,
      'jobId': 'thejobid-%s' % target
    },
    'configuration': {
      'load': {
        'sourceUris': [source],
        'schema': {
          'fields': [
            {'name': 'language', 'type': 'STRING'},
            {'name': 'title', 'type': 'STRING'},
            {'name': 'requests', 'type': 'INTEGER'},
            {'name': 'content_size', 'type': 'INTEGER'}
          ]
        },
        'destinationTable': {
          'projectId': PROJECT_ID,
          'datasetId': dataset,
          'tableId': target
        },
        'writeDisposition': 'WRITE_TRUNCATE',
        'fieldDelimiter': ' ',
        'quote': '',
      }
    }
  }
  insert_response = service.jobs().insert(
      projectId=PROJECT_ID, body=job_data).execute()








