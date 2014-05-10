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
import csv
import json

import webapp2
from google.appengine.api import app_identity
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from apiclient.discovery import build
import httplib2
from oauth2client import appengine


SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_ID = app_identity.get_application_id()

credentials = appengine.AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
service = build('bigquery', 'v2', http=http)


class MainHandler(webapp2.RequestHandler):
  def get(self):
    output_format = self.request.get('_format', 'csv_text')
    query_key = self.request.get('query', QUERIES.keys()[0])
    period_key = self.request.get('period', PERIODS.keys()[0])

    self.response.headers.add_header('Access-Control-Allow-Origin', '*')
    self.response.headers['Content-Type'] = {'csv': 'text/csv'}.get(format, 'text/plain')
  
    query = QUERIES[query_key] % PERIODS[period_key]
    query_results = runSyncQuery(service, PROJECT_ID, query)
    if output_format.startswith('csv'):
      writer = csv.writer(self.response.out)
      for row in query_results:
        writer.writerow(row)
    if output_format == 'json':
      result = {'query': query}
      result['results'] = [{'data': data, 'period': period_key}]
      self.response.out.write(json.dumps(result))


class GetConfigHandler(webapp2.RequestHandler):
  def get(self):
    self.response.headers.add_header('Access-Control-Allow-Origin', '*')
    result = {
      'queries': sorted(QUERIES.keys()),
      'periods': sorted(PERIODS.keys()),
    }
    self.response.out.write(json.dumps(result))
    


app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/get_config', GetConfigHandler),
], debug=True)


def runSyncQuery (service, projectId, query, timeout=0):
    jobCollection = service.jobs()
    queryData = {'query':query, 'timeoutMs':timeout}
    queryReply = jobCollection.query(
        projectId=projectId, body=queryData).execute()
    jobReference=queryReply['jobReference']

    while(not queryReply['jobComplete']):
      queryReply = jobCollection.getQueryResults(
          projectId=jobReference['projectId'],
          jobId=jobReference['jobId'],
          timeoutMs=timeout).execute()

    results = []
    if('rows' in queryReply):
      bqToPlainArray(queryReply, 0, results)
      currentRow = len(queryReply['rows'])

      # Loop through each page of data
      while('rows' in queryReply and currentRow < queryReply['totalRows']):
        queryReply = jobCollection.getQueryResults(
            projectId=jobReference['projectId'],
            jobId=jobReference['jobId'],
            startIndex=currentRow).execute()
        if('rows' in queryReply):
          bqToPlainArray(queryReply, currentRow, results)
          currentRow += len(queryReply['rows'])
    return results



def bqToPlainArray(reply, rowNumber, results):
  for row in reply['rows']:
    results += [[x['v'] for x in row['f']]]


QUERIES = {}
QUERIES['women_by_country'] = """SELECT title, count, iso FROM (
SELECT title, count, c.iso iso, RANK() OVER (PARTITION BY iso ORDER BY count DESC) rank
FROM (
 SELECT a.title title, SUM(requests) count, b.person person
 FROM [%s] a
 JOIN (
   SELECT REGEXP_REPLACE(obj, '/wikipedia/id/', '') title, a.sub person
   FROM [fh-bigquery:freebase20140119.triples_nolang] a
   JOIN (
     SELECT sub FROM [fh-bigquery:freebase20140119.people_gender]
     WHERE gender='/m/02zsn') b
   ON a.sub=b.sub
   WHERE obj CONTAINS '/wikipedia/id/' AND pred = '/type/object/key'
   GROUP BY 1,2) b
 ON a.title = b.title 
 GROUP BY 1,3) a
JOIN EACH [fh-bigquery:freebase20140119.people_place_of_birth] b
ON a.person=b.sub
JOIN [fh-bigquery:freebase20140119.place_of_birth_to_country] c
ON b.place_of_birth=c.place
)
WHERE rank=1
ORDER BY count DESC;"""

PERIODS = {}
PERIODS['2013/08'] = 'fh-bigquery:wikipedia.wikipedia_views_201308'
PERIODS['2014/02/01 00:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140201_00'
PERIODS['2014/02/11 21:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140211_21'
PERIODS['2014/02/12 20:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140212_20'
PERIODS['2014/02/12 21:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140212_21'
PERIODS['2014/02/12 22:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140212_22'
PERIODS['2014/02/12 23:00 UTC'] = 'fh-bigquery:wikipedia.wikipedia_views_20140212_23'
PERIODS['2014/04/10 10:00 UTC'] = 'fh-bigquery:wikipedia.pagecounts_20140410_150000'
PERIODS['2014/04/11 11:00 UTC'] = 'fh-bigquery:wikipedia.pagecounts_20140411_080000'
PERIODS['2014/04/12 13:00 UTC'] = 'fh-bigquery:wikipedia.pagecounts_20140412_130000'
PERIODS['2014/04/12 14:00 UTC'] = 'fh-bigquery:wikipedia.pagecounts_20140412_140000'


