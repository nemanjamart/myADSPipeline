import unittest
import os
import httpretty
from sqlalchemy.sql.expression import and_
from datetime import timedelta

import adsputils as utils
from myadsp import app
from myadsp.models import AuthorInfo, Results, Base


class TestmyADSCelery(unittest.TestCase):
    """
    Tests the application's methods
    """

    postgresql_url_dict = {
        'port': 5432,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test_myadspipeline'
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
    def test_get_users(self):
        app = self.app
        since = utils.get_date('2000-01-02')

        with self.app.session_scope() as session:
            u1 = AuthorInfo(id=1, created=since, last_sent_daily=since, last_sent_weekly=since)
            session.add(u1)
            session.commit()

        # if there's an error getting the newest users, still return existing users
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_VAULT_MYADS_USERS'] % since.isoformat(),
            content_type='application/json',
            status=404,
            body='{"users":[2,3]}'
        )

        users = app.get_users(since=since, frequency='daily')
        self.assertEqual([1], users)

        # return both new and existing users if possible
        httpretty.register_uri(
            httpretty.GET, self.app.conf['API_VAULT_MYADS_USERS'] % since.isoformat(),
            content_type='application/json',
            status=200,
            body='{"users":[2,3]}'
        )

        users = app.get_users(since=since, frequency='daily')
        self.assertEqual([1,2,3], users)

    def test_get_recent_results(self):
        app = self.app
        created_1 = utils.get_date('2019-01-01')
        created_2 = utils.get_date('2019-02-01')
        today = utils.get_date()
        created_3 = today - timedelta(self.app.conf['STATEFUL_RESULTS_DAYS'] - 1)

        with self.app.session_scope() as session:
            old_res_1 = Results(user_id=2, qid='1234567890abcdefghijklmnopqrstuv', results=['bib1'], created=created_1)
            old_res_2 = Results(user_id=2, qid='1234567890abcdefghijklmnopqrstuv', results=['bib2','bib3'], created=created_2)
            newish_res = Results(user_id=2, qid='1234567890abcdefghijklmnopqrstuv', results=['bib4'], created=created_3)
            session.add(old_res_1)
            session.add(old_res_2)
            session.add(newish_res)
            session.commit()

        input_res = ['bib1', 'bib2', 'bib3', 'bib4', 'bib5']
        new_res = app.get_recent_results(user_id=2, qid='1234567890abcdefghijklmnopqrstuv', input_results=input_res, ndays=self.app.conf['STATEFUL_RESULTS_DAYS'])

        # only new results are returned, including results more recent than STATEFUL_RESULTS_DAYS
        self.assertEqual(set(new_res), set(['bib4', 'bib5']))

        with self.app.session_scope() as session:
            new_stored = session.query(Results).filter(and_(Results.qid == '1234567890abcdefghijklmnopqrstuv',
                                                            Results.user_id == 2,
                                                            Results.created >= today)).all()

            new_bibc = []
            for s in new_stored:
                new_bibc += s.results

        # new results are stored, excluding results more recent than STATEFUL_RESULTS_DAYS
        self.assertEqual(new_bibc, ['bib5'])

        with self.app.session_scope() as session:
            old_res_1 = Results(user_id=2, setup_id=123, results=['bib1'], created=created_1)
            old_res_2 = Results(user_id=2, setup_id=123, results=['bib2', 'bib3'], created=created_2)
            newish_res = Results(user_id=2, setup_id=123, results=['bib4'], created=created_3)
            session.add(old_res_1)
            session.add(old_res_2)
            session.add(newish_res)
            session.commit()

        input_res = ['bib1', 'bib2', 'bib3', 'bib4', 'bib5']
        new_res = app.get_recent_results(user_id=2, setup_id=123, input_results=input_res, ndays=self.app.conf['STATEFUL_RESULTS_DAYS'])

        # only new results are returned, including results more recent than STATEFUL_RESULTS_DAYS
        self.assertEqual(set(new_res), set(['bib4', 'bib5']))

        with self.app.session_scope() as session:
            new_stored = session.query(Results).filter(and_(Results.setup_id == 123,
                                                            Results.user_id == 2,
                                                            Results.created >= today)).all()

            new_bibc = []
            for s in new_stored:
                new_bibc += s.results

        # new results are stored, excluding results more recent than STATEFUL_RESULTS_DAYS
        self.assertEqual(new_bibc, ['bib5'])


if __name__ == '__main__':
    unittest.main()