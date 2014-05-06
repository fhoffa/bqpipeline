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
import webapp2

class MainHandler(webapp2.RequestHandler):
    def get(self):
      doIt(self)


app = webapp2.WSGIApplication([
    ('/', MainHandler)
], debug=True)




import pprint

from apiclient.discovery import build
import httplib2

from google.appengine.api import app_identity
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client import appengine


SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_ID = app_identity.get_application_id()

credentials = appengine.AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
service = build('bigquery', 'v2', http=http)


# Run a synchronous query, save the results to a table, overwriting the
# existing data, and print the first page of results.
# Default timeout is to wait until query finishes.
def runSyncQuery (service, projectId, query, timeout=0):
    jobCollection = service.jobs()
    queryData = {'query':query, 'timeoutMs':timeout}
    queryReply = jobCollection.query(projectId=projectId,
                                     body=queryData).execute()

    jobReference=queryReply['jobReference']

    # Timeout exceeded: keep polling until the job is complete.
    while(not queryReply['jobComplete']):
      print 'Job not yet complete...'
      queryReply = jobCollection.getQueryResults(
                          projectId=jobReference['projectId'],
                          jobId=jobReference['jobId'],
                          timeoutMs=timeout).execute()

    # If the result has rows, print the rows in the reply.
    results = []
    if('rows' in queryReply):
      printTableData(queryReply, 0, results)
      currentRow = len(queryReply['rows'])

      # Loop through each page of data
      while('rows' in queryReply and currentRow < queryReply['totalRows']):
        queryReply = jobCollection.getQueryResults(
                          projectId=jobReference['projectId'],
                          jobId=jobReference['jobId'],
                          startIndex=currentRow).execute()
        if('rows' in queryReply):
          printTableData(queryReply, currentRow, results)
          currentRow += len(queryReply['rows'])
    return results



def printTableData(reply, rowNumber, results):
  for row in reply['rows']:
    results += [[x['v'] for x in row['f']]]


def doIt(web):
  self.response.headers.add_header("Access-Control-Allow-Origin", "*")
  self.response.headers['Content-Type'] = 'text/csv'
  query = """SELECT title, count, iso FROM (
SELECT title, count, c.iso iso, RANK() OVER (PARTITION BY iso ORDER BY count DESC) rank
FROM (
 SELECT a.title title, SUM(requests) count, b.person person
 FROM [fh-bigquery:wikipedia.pagecounts_20140412_140000] a
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
  writer = csv.writer(self.response.out)
  for row in runSyncQuery(service, PROJECT_ID, query):
    writer.writerow(["foo", "foo,bar", "bar"])


