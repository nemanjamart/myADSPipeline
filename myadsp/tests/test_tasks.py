import unittest
import sys
import os
import json
import httpretty
from mock import patch

import adsputils
from myadsp import app, utils, tasks
from myadsp.models import Base, AuthorInfo
from ..emails import myADSTemplate

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

    @httpretty.activate
    def test_task_process_myads(self):
        msg = {'frequency': 'daily'}

        # can't process without a user ID
        with patch.object(tasks.logger, 'error', return_value=None) as logger:
            tasks.task_process_myads(msg)
            logger.assert_called_with(u"No user ID received for {0}".format(msg))

        msg = {'userid': 123}

        # can't process without a frequency
        with patch.object(tasks.logger, 'error', return_value=None) as logger:
            tasks.task_process_myads(msg)
            logger.assert_called_with(u"No frequency received for {0}".format(msg))

        # process a user (the user should get created during the task)
        msg = {'userid': 123, 'frequency': 'daily'}

        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_VAULT_MYADS_SETUP'] % msg['userid'],
            content_type='application/json',
            status=200,
            body=json.dumps([{'name': 'Query 1',
                              'qid': '1234567890abcdefghijklmnopqrstu1',
                              'active': True,
                              'stateful': True,
                              'frequency': 'daily',
                              'type': 'query'},
                             {'name': 'Query 2',
                              'qid': '1234567890abcdefghijklmnopqrstu2',
                              'active': True,
                              'stateful': False,
                              'frequency': 'weekly',
                              'type': 'query'},
                             {'name': 'Query 3',
                              'qid': '1234567890abcdefghijklmnopqrstu3',
                              'active': True,
                              'stateful': False,
                              'frequency': 'weekly',
                              'type': 'template',
                              'template': 'authors',
                              'data': {'data': 'author:Kurtz'}}
                             ])
        )

        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_VAULT_EXECUTE_QUERY'] % ('1234567890abcdefghijklmnopqrstu1', 'bibcode,title,author_norm', 10),
            content_type='application/json',
            status=200,
            body=json.dumps({u'response': {u'docs': [{u'bibcode': u'2019arXiv190800829P',
                                                      u'title': [u'Gravitational wave signatures from an extended ' +
                                                                 u'inert doublet dark matter model'],
                                                      u'author_norm': [u'Paul, A', u'Banerjee, B', u'Majumdar, D'],
                                                      u"identifier": [u"2019arXiv190800829P", u"arXiv:1908.00829"],
                                                      u"year": u"2019",
                                                      u"bibstem": [u"arXiv"]},
                                                     {u'bibcode': u'2019arXiv190800678L',
                                                      u'title': [u'Prospects for Gravitational Wave Measurement ' +
                                                                 u'of ZTFJ1539+5027'],
                                                      u'author_norm': [u'Littenberg, T', u'Cornish, N'],
                                                      u"identifier": [u"2019arXiv190800678L", u"arXiv:1908.00678"],
                                                      u"year": u"2019",
                                                      u"bibstem": [u"arXiv"]}],
                                           u'numFound': 2,
                                           u'start': 0},
                            u'responseHeader': {u'QTime': 5,
                                                u'params': {u'fl': u'bibcode,title,author_norm,identifier,year,bibstem',
                                                            u'q': u'title:"gravity waves" ' +
                                                                  u'entdate:[2019-08-03 TO 2019-08-04] bibstem:"arxiv"',
                                                            u'rows': u'2',
                                                            u'start': u'0',
                                                            u'wt': u'json',
                                                            u'x-amzn-trace-id':
                                                                u'Root=1-5d3b6518-3b417bec5eee25783a4147f4'},
                                                u'status': 0}})
        )

        with patch.object(self.app, 'get_recent_results') as get_recent_results, \
            patch.object(utils, 'get_user_email') as get_user_email, \
            patch.object(utils, 'payload_to_plain') as payload_to_plain, \
            patch.object(utils, 'payload_to_html') as payload_to_html, \
            patch.object(utils, 'send_email') as send_email:

            get_recent_results.return_value = ['2019arXiv190800829P', '2019arXiv190800678L']
            get_user_email.return_value = 'test@test.com'
            payload_to_plain.return_value = 'plain payload'
            payload_to_html.return_value = '<em>html payload</em>'
            send_email.return_value = 'this should be a MIMEMultipart object'

            tasks.task_process_myads(msg)
            with self.app.session_scope() as session:
                user = session.query(AuthorInfo).filter_by(id=123).first()
                self.assertEqual(adsputils.get_date().date(), user.last_sent.date())

        msg = {'userid': 123, 'frequency': 'weekly', 'force': False}

        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_VAULT_EXECUTE_QUERY'] % ('1234567890abcdefghijklmnopqrstu2', 'bibcode,title,author_norm', 10),
            content_type='application/json',
            status=200,
            body=json.dumps({u'response': {u'docs': [{u'bibcode': u'2019arXiv190800829P',
                                                      u'title': [u'Gravitational wave signatures from an ' +
                                                                 u'extended inert doublet dark matter model'],
                                                      u'author_norm': [u'Paul, A', u'Banerjee, B', u'Majumdar, D'],
                                                      u"identifier": [u"2019arXiv190800829P", u"arXiv:1908.00829"],
                                                      u"year": u"2019",
                                                      u"bibstem": [u"arXiv"]},
                                                     {u'bibcode': u'2019arXiv190800678L',
                                                      u'title': [u'Prospects for Gravitational Wave Measurement ' +
                                                                 u'of ZTFJ1539+5027'],
                                                      u'author_norm': [u'Littenberg, T', u'Cornish, N'],
                                                      u"identifier": [u"2019arXiv190800678L", u"arXiv:1908.00678"],
                                                      u"year": u"2019",
                                                      u"bibstem": [u"arXiv"]}],
                                           u'numFound': 2,
                                           u'start': 0},
                             u'responseHeader': {u'QTime': 5,
                                                 u'params': {u'fl': u'bibcode,title,author_norm',
                                                             u'fq': u'{!bitset}',
                                                             u'q': u'*:*',
                                                             u'rows': u'2',
                                                             u'start': u'0',
                                                             u'wt': u'json',
                                                             u'x-amzn-trace-id':
                                                                 u'Root=1-5d3b6518-3b417bec5eee25783a4147f4'},
                                                 u'status': 0}})
        )
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_SOLR_QUERY_ENDPOINT']+'?q={0}&sort={1}&fl={2}&rows={3}'.
            format('author:Kurtz', 'score+desc+bibcode+desc', 'bibcode,title,author_norm', 5),
            content_type='application/json',
            status=200,
            body=json.dumps({"responseHeader": {"status": 0,
                                                "QTime": 23,
                                                "params": {"q": "author:Kurtz",
                                                           "x-amzn-trace-id": "Root=1-5d769c6c-f96bfa49d348f03d8ecb7464",
                                                           "fl": "bibcode,title,author_norm",
                                                           "start": "0",
                                                           "sort": "score desc",
                                                           "rows": "5",
                                                           "wt": "json"}},
                             "response": {"numFound": 2712,
                                          "start": 0,
                                          "docs": [{"bibcode": "1971JVST....8..324K",
                                                    "title": ["High-Capacity Lead Tin Barrel Dome..."],
                                                    "author_norm": ["Kurtz, J"],
                                                    "identifier": ["1971JVST....8..324K"],
                                                    "year": "1971",
                                                    "bibstem": ["JVST"]},
                                                   {"bibcode": "1972ApJ...178..701K",
                                                    "title": ["Search for Coronal Line Emission from the Cygnus Loop"],
                                                    "author_norm": ["Kurtz, D", "Vanden Bout, P", "Angel, J"],
                                                    "identifier": ["1972ApJ...178..701K"],
                                                    "year": "1972",
                                                    "bibstem": ["ApJ"]},
                                                   {"bibcode": "1973ApOpt..12..891K",
                                                    "title": ["Author's Reply to Comments on: Experimental..."],
                                                    "author_norm":["Kurtz, R"],
                                                    "identifier": ["1973ApOpt..12..891K"],
                                                    "year": "1973",
                                                    "bibstem": ["ApOpt"]},
                                                   {"bibcode": "1973SSASJ..37..725W",
                                                    "title": ["Priming Effect of 15N-Labeled Fertilizers..."],
                                                    "author_norm": ["Westerman, R","Kurtz, L"],
                                                    "identifier": ["1973SSASJ..37..725W"],
                                                    "year": "1973",
                                                    "bibstem": ["SSASJ"]},
                                                   {"bibcode": "1965JSpRo...2..818K",
                                                    "title": ["Orbital tracking and decay analysis of the saturn..."],
                                                    "author_norm":["Kurtz, H", "McNair, A", "Naumcheff, M"],
                                                    "identifier": ["1965JSpRo...2..818K"],
                                                    "year": "1965",
                                                    "bibstem": ["JSpRo"]}]}})
        )

        with patch.object(self.app, 'get_recent_results') as get_recent_results, \
            patch.object(utils, 'get_user_email') as get_user_email, \
            patch.object(utils, 'payload_to_plain') as payload_to_plain, \
            patch.object(utils, 'payload_to_html') as payload_to_html, \
            patch.object(utils, 'send_email') as send_email:

            get_recent_results.return_value = ['2019arXiv190800829P', '2019arXiv190800678L']
            get_user_email.return_value = 'test@test.com'
            payload_to_plain.return_value = 'plain payload'
            payload_to_html.return_value = '<em>html payload</em>'
            send_email.return_value = 'this should be a MIMEMultipart object'

            # already ran today, tried to run again without force=True
            with patch.object(tasks.logger, 'warning', return_value=None) as logger:
                tasks.task_process_myads(msg)
                logger.assert_called_with(u"Email for user {0} already sent today".format(msg['userid']))

            msg = {'userid': 123, 'frequency': 'weekly'}

            # reset user
            with self.app.session_scope() as session:
                user = session.query(AuthorInfo).filter_by(id=123).first()
                user.last_sent = None
                session.add(user)
                session.commit()

            with self.app.session_scope() as session:
                user = session.query(AuthorInfo).filter_by(id=123).first()
                self.assertIsNone(user.last_sent)

            tasks.task_process_myads(msg)

            with self.app.session_scope() as session:
                user = session.query(AuthorInfo).filter_by(id=123).first()
                self.assertEqual(adsputils.get_date().date(), user.last_sent.date())