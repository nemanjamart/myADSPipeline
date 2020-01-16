import unittest
import os
import httpretty
from mock import patch
import urllib
import json
import datetime

import adsputils as utils
from myadsp import app, utils
from myadsp.models import Base
from ..emails import myADSTemplate

payload = [{'name': 'Query 1',
                    'query_url': 'https://path/to/query',
                    'results': [{"author_norm": ["Nantais, J", "Huchra, J"],
                                 "bibcode":"2012yCat..51392620N",
                                 "title":["VizieR Online Data Catalog: Spectroscopy of M81 globular clusters"],
                                 "year": "2012",
                                 "bibstem": ["yCat"]},
                                {"author_norm": ["Huchra, J", "Macri, L"],
                                 "bibcode":"2012ApJS..199...26H",
                                 "title":["The 2MASS Redshift Survey Description and Data Release"],
                                 "year": "2012",
                                 "bibstem": ["ApJS"]}]},
           {'name': 'Query 2',
                    'query_url': 'https://path/to/query',
                    'results': [{"author_norm": ["Nantais, J", "Huchra, J"],
                                 "bibcode": "2012yCat..51392620N",
                                 "title": ["VizieR Online Data Catalog: Spectroscopy of M81 globular clusters"],
                                 "year": "2012",
                                 "bibstem": ["yCat"]},
                                {"author_norm": ["Huchra, J", "Macri, L"],
                                 "bibcode": "2012ApJS..199...26H",
                                 "title": ["The 2MASS Redshift Survey Description and Data Release"],
                                 "year": "2012",
                                 "bibstem": ["ApJS"]}]}]


class TestmyADSCelery(unittest.TestCase):
    """
    Tests the application's methods
    """

    postgresql_url_dict = {
        'port': 5432,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'myads_pipeline'
    }
    postgresql_url = 'postgresql://{user}:{user}@{host}:{port}/{database}' \
        .format(user=postgresql_url_dict['user'],
                host=postgresql_url_dict['host'],
                port=postgresql_url_dict['port'],
                database=postgresql_url_dict['database']
    )

    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.myADSCelery('test', local_config={'SQLALCHEMY_URL': self.postgresql_url,
                                                         'SQLALCHEMY_ECHO': False,
                                                         'PROJ_HOME': proj_home,
                                                         'TEST_DIR': os.path.join(proj_home, 'myadsp/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()

    def test_app(self):
        assert self.app._config.get('SQLALCHEMY_URL') == self.postgresql_url
        assert self.app.conf.get('SQLALCHEMY_URL') == self.postgresql_url

    def test_send_email(self):
        email_addr = 'to@test.com'
        payload_plain = 'plain test'
        payload_html = '<em>html test</em>'
        with patch('smtplib.SMTP') as mock_smtp:
            msg = utils.send_email(email_addr,
                                   email_template=myADSTemplate,
                                   payload_plain=payload_plain,
                                   payload_html=payload_html)

            self.assertTrue(payload_plain in msg.get_payload()[0].get_payload())
            self.assertTrue(payload_html in msg.get_payload()[1].get_payload())
            self.assertTrue(myADSTemplate.subject == msg.get('subject'))

    @httpretty.activate
    def test_get_user_email(self):
        user_id = 1

        httpretty.register_uri(
            httpretty.GET, self.app._config.get('API_ADSWS_USER_EMAIL') % user_id,
            content_type='application/json',
            status=200,
            body='{"id": 1, "email": "test@test.com"}'
        )

        email = utils.get_user_email(userid=None)

        self.assertIsNone(email)

        email = utils.get_user_email(userid=user_id)

        self.assertEquals(email, 'test@test.com')

    @httpretty.activate
    def test_get_query_results(self):
        myADSsetup = {'name': 'Test Query',
                      'qid': 1,
                      'active': True,
                      'stateful': False,
                      'frequency': 'weekly',
                      'type': 'query',
                      'rows': 5,
                      'fields': 'bibcode,title,author_norm'}

        httpretty.register_uri(
            httpretty.GET, self.app._config.get('API_VAULT_EXECUTE_QUERY') % (1, myADSsetup['fields'], 5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {"q": "author:Kurtz",
                                                           "fl": "bibcode,title,author_norm",
                                                           "start": "0",
                                                           "sort": "score desc",
                                                           "rows": "5",
                                                           "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": ["High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"]}]}})
        )

        results = utils.get_query_results(myADSsetup)

        self.assertEqual(results, [{'name': myADSsetup['name'],
                                    'query_url': self.app._config.get('QUERY_ENDPOINT') %
                                    urllib.urlencode({"q": "author:Kurtz",
                                                      "sort": "score desc"}),
                                    'results': [{"bibcode": "1971JVST....8..324K",
                                                 "title": ["High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 "author_norm": ["Kurtz, J"]}],
                                    "query": "author:Kurtz"
                                    }])

    @httpretty.activate
    def test_get_template_query_results(self):
        # test arxiv query
        myADSsetup = {'name': 'Test Query - arxiv',
                      'qid': 1,
                      'active': True,
                      'stateful': False,
                      'frequency': 'weekly',
                      'type': 'template',
                      'template': 'arxiv',
                      'data': 'AGN',
                      'classes': ['astro-ph'],
                      'fields': 'bibcode,title,author_norm,identifier',
                      'rows': 2000}

        start = (utils.get_date() - datetime.timedelta(days=1)).date()
        end = utils.get_date().date()
        start_year = (utils.get_date() - datetime.timedelta(days=180)).year
        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                         format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                                query=urllib.quote_plus('bibstem:arxiv ((arxiv_class:astro-ph.*) OR (AGN)) '
                                                        'entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year)),
                                sort=urllib.quote_plus('score desc'),
                                fields='bibcode,title,author_norm,identifier',
                                rows=2000),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {"q": "bibstem:arxiv ((arxiv_class:astro-ph.*) OR (AGN)) "
                                                                'entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year),
                                                           "fl": "bibcode,title,author_norm,identifier",
                                                           "start": "0",
                                                           "sort": "score desc",
                                                           "rows": "5",
                                                           "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "identifier": ["1971JVST....8..324K", "arXiv:1234:5678"],
                                                    "title": ["High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"]}]}})
        )

        results = utils.get_template_query_results(myADSsetup)
        start = (utils.get_date() - datetime.timedelta(days=25)).date()
        end = utils.get_date().date()
        self.assertEqual(results, [{'name': myADSsetup['name'],
                                    'query': 'bibstem:arxiv ((arxiv_class:astro-ph.*) (AGN)) '
                                             'entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year),
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('bibstem:arxiv ((arxiv_class:astro-ph.*) (AGN)) '
                                                  'entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year)),
                                urllib.quote_plus("score desc, bibcode desc")),
                                    'results': [{u'arxiv_id': u'arXiv:1234:5678',
                                                 u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u'identifier': [u'1971JVST....8..324K', u'arXiv:1234:5678']}]}])

        # test citations query
        myADSsetup = {'name': 'Test Query - citations',
                      'qid': 1,
                      'active': True,
                      'stateful': True,
                      'frequency': 'weekly',
                      'type': 'template',
                      'template': 'citations',
                      'data': 'author:Kurtz OR author:"Kurtz, M."',
                      'fields': 'bibcode,title,author_norm,identifier',
                      'rows': 5}

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                       query=urllib.quote_plus(
                           'citations(author:Kurtz OR author:"Kurtz, M.")'),
                       sort=urllib.quote_plus('date desc, bibcode desc'),
                       fields='bibcode,title,author_norm,identifier',
                       rows=5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {
                                                    "q": 'citations(author:Kurtz OR author:"Kurtz, M.")',
                                                    "fl": "bibcode,title,author_norm,identifier,year,bibstem",
                                                    "start": "0",
                                                    "sort": "date desc, bibcode desc",
                                                    "rows": "5",
                                                    "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": [
                                                        "High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]}]},
                             'stats': {u'stats_fields': {u'citation_count': {u'count': 6145,
                                                                             u'max': 3467.0,
                                                                             u'mean': 26.28006509357201,
                                                                             u'min': 0.0,
                                                                             u'missing': 14,
                                                                             u'stddev': 81.05058763022076,
                                                                             u'sum': 161491.0,
                                                                             u'sumOfSquares': 44605145.0}}}
                             })
        )

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&rows=1&stats=true&stats.field=citation_count'. \
                               format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                                      query=urllib.quote_plus('author:Kurtz')),
            content_type='application/json',
            status=200,
            body=json.dumps({"response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": [
                                                        "High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]}]},
                              'stats': {u'stats_fields': {u'citation_count': {u'count': 6145,
                                                                              u'max': 3467.0,
                                                                              u'mean': 26.28006509357201,
                                                                              u'min': 0.0,
                                                                              u'missing': 14,
                                                                              u'stddev': 81.05058763022076,
                                                                              u'sum': 161491.0,
                                                                              u'sumOfSquares': 44605145.0}}}})
        )

        results = utils.get_template_query_results(myADSsetup)
        self.assertEqual(results, [{'name': 'Test Query - citations (Citations: 161491)',
                                    'query': 'citations(author:Kurtz OR author:"Kurtz, M.")',
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('citations(author:Kurtz OR author:"Kurtz, M.")'),
                                urllib.quote_plus("entry_date desc, bibcode desc")),
                                    'results': [{u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [
                                                     u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u"identifier": [u"1971JVST....8..324K"],
                                                 u"year": u"1971",
                                                 u"bibstem": [u"JVST"]}]}])

        # test authors query
        myADSsetup = {'name': 'Test Query - authors',
                      'qid': 1,
                      'active': True,
                      'stateful': True,
                      'frequency': 'weekly',
                      'type': 'template',
                      'template': 'authors',
                      'data': 'database:astronomy author:Kurtz',
                      'fields': 'bibcode,title,author_norm,identifier',
                      'rows': 5}

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                       query=urllib.quote_plus(
                           'author:Kurtz'),
                       sort=urllib.quote_plus('score desc, bibcode desc'),
                       fields='bibcode,title,author_norm,identifier',
                       rows=5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {
                                                    "q": "database:astronomy author:Kurtz",
                                                    "fl": "bibcode,title,author_norm,identifier,year,bibstem",
                                                    "start": "0",
                                                    "sort": "score desc, bibcode desc",
                                                    "rows": "5",
                                                    "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": [
                                                        "High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]}]}})
        )

        results = utils.get_template_query_results(myADSsetup)
        self.assertEqual(results, [{'name': myADSsetup['name'],
                                    'query': 'database:astronomy author:Kurtz entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year),
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('database:astronomy author:Kurtz entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year)),
                                urllib.quote_plus("score desc, bibcode desc")),
                                    'results': [{u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [
                                                     u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u"identifier": [u"1971JVST....8..324K"],
                                                 u"year": u"1971",
                                                 u"bibstem": [u"JVST"]}]}])

        # test keyword query
        myADSsetup = {'name': 'Test Query - keywords',
                      'qid': 1,
                      'active': True,
                      'stateful': True,
                      'frequency': 'weekly',
                      'type': 'template',
                      'template': 'keyword',
                      'data': 'AGN',
                      'classes': ['astro-ph', 'physics.space-ph'],
                      'fields': 'bibcode,title,author_norm,identifier',
                      'rows': 5}

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                       query=urllib.quote_plus('(arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN'),
                       sort=urllib.quote_plus('entry_date desc, bibcode desc'),
                       fields='bibcode,title,author_norm,identifier',
                       rows=5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {
                                                    "q": "(arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN",
                                                    "fl": "bibcode,title,author_norm,identifier,year,bibstem",
                                                    "start": "0",
                                                    "sort": "entry_date desc, bibcode desc",
                                                    "rows": "5",
                                                    "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": ["High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]
                                                    }]}})
        )

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                       query=urllib.quote_plus('trending((arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN)'),
                       sort=urllib.quote_plus('score desc, bibcode desc'),
                       fields='bibcode,title,author_norm,identifier',
                       rows=5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {
                                                    "q": "trending((arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN)",
                                                    "fl": "bibcode,title,author_norm,identifier,year,bibstem",
                                                    "start": "0",
                                                    "sort": "score desc, bibcode desc",
                                                    "rows": "5",
                                                    "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": [
                                                        "High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]}]}})
        )

        httpretty.register_uri(
            httpretty.GET, '{endpoint}?q={query}&sort={sort}&fl={fields}&rows={rows}'.
                format(endpoint=self.app._config.get('API_SOLR_QUERY_ENDPOINT'),
                       query=urllib.quote_plus('useful((arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN)'),
                       sort=urllib.quote_plus('score desc, bibcode desc'),
                       fields='bibcode,title,author_norm,identifier',
                       rows=5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {
                                                    "q": "useful((arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) AGN)",
                                                    "fl": "bibcode,title,author_norm,identifier,year,bibstem",
                                                    "start": "0",
                                                    "sort": "score desc, bibcode desc",
                                                    "rows": "5",
                                                    "wt": "json"}},
                             "response": {"numFound": 1,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": [
                                                        "High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]}]}})
        )

        results = utils.get_template_query_results(myADSsetup)
        self.assertEqual(results, [{'name': 'Test Query - keywords - Recent Papers',
                                    'query': 'AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year),
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph) entdate:["{0}Z00:00" TO "{1}Z00:00"] pubdate:[{2}-00 TO *]'.format(start, end, start_year)),
                                urllib.quote_plus("entry_date desc, bibcode desc")),
                                    'results': [{u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u"identifier": [u"1971JVST....8..324K"],
                                                 u"year": u"1971",
                                                 u"bibstem": [u"JVST"]}]},
                                   {'name': 'Test Query - keywords - Most Popular',
                                    'query': 'trending(AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph))',
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('trending(AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph))'),
                                urllib.quote_plus("score desc, bibcode desc")),
                                    'results': [{u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [
                                                     u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u"identifier": [u"1971JVST....8..324K"],
                                                 u"year": u"1971",
                                                 u"bibstem": [u"JVST"]}]},
                                   {'name': 'Test Query - keywords - Most Cited',
                                    'query': 'useful(AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph))',
                                    'query_url': 'https://ui.adsabs.harvard.edu/search/q={0}&sort={1}'.
                         format(urllib.quote_plus('useful(AGN (arxiv_class:astro-ph.* OR arxiv_class:physics.space-ph))'),
                                urllib.quote_plus("score desc, bibcode desc")),
                                    'results': [{u"bibcode": u"1971JVST....8..324K",
                                                 u"title": [
                                                     u"High-Capacity Lead Tin Barrel Dome Production Evaporator"],
                                                 u"author_norm": [u"Kurtz, J"],
                                                 u"identifier": [u"1971JVST....8..324K"],
                                                 u"year": u"1971",
                                                 u"bibstem": [u"JVST"]}]}
                                   ])

    def test_get_first_author_formatted(self):
        results_dict = {"bibcode": "2012ApJS..199...26H",
                        "title": ["The 2MASS Redshift Survey: Description and Data Release"],
                        "author_norm": ["Huchra, J", "Macri, L", "Masters, K", "Jarrett, T"]}

        first_author = utils._get_first_author_formatted(results_dict, author_field='author_norm')
        self.assertEquals(first_author, 'Huchra, J; Macri, L; Masters, K and 1 more')

        results_dict = {"bibcode": "2012ApJS..199...26H",
                        "title": ["The 2MASS Redshift Survey: Description and Data Release"],
                        "author_norm": ["Huchra, J"]}

        first_author = utils._get_first_author_formatted(results_dict, author_field='author_norm')
        self.assertEquals(first_author, 'Huchra, J')

    def test_payload_to_plain(self):

        formatted_payload = utils.payload_to_plain(payload)

        split_payload = formatted_payload.split('\n')
        self.assertEquals(split_payload[0].strip(), 'Query 1 (https://path/to/query)')
        self.assertEquals(split_payload[1].strip(), '"VizieR Online Data Catalog: Spectroscopy of M81 globular ' +
                                                    'clusters," Nantais, J and Huchra, J (2012yCat..51392620N)')

    def test_payload_to_html(self):

        formatted_payload = utils.payload_to_html(payload, col=1, email_address="test@tester.com")

        split_payload = formatted_payload.split('\n')
        self.assertIn(u'templateColumnContainer"', split_payload[57])
        self.assertEquals(split_payload[62].strip(),
                          u'<h3><a href="https://path/to/query" title="" style="color: #1C459B; font-style: italic;' +
                          u'font-weight: bold;">Query 1</a></h3>')
        self.assertIn(u'href="https://ui.adsabs.harvard.edu/abs/2012yCat..51392620N/abstract"', split_payload[66])

        formatted_payload = utils.payload_to_html(payload, col=2)

        split_payload = formatted_payload.split('\n')

        self.assertIn(u'class="leftColumnContent"', split_payload[60])
        self.assertEquals(split_payload[62].strip(),
                          u'<h3><a href="https://path/to/query" title="" style="color: #1C459B; font-style: italic;' +
                          u'font-weight: bold;">Query 1</a></h3>')
        self.assertIn(u'href="https://ui.adsabs.harvard.edu/abs/2012yCat..51392620N/abstract"', split_payload[65])

        formatted_payload = utils.payload_to_html(payload, col=3)
        self.assertIsNone(formatted_payload)


















