from adsputils import get_date, ADSCelery
from .models import AuthorInfo, Results

import requests
from datetime import timedelta
from sqlalchemy.sql.expression import and_

class myADSCelery(ADSCelery):

    def get_users(self, since='1971-01-01T12:00:00Z'):
        """
        Checks internal storage and vault for all existing and new/updated myADS users. Adds new users to authors table (last_sent should be blank)
        :param app:
        :param since: used to fetch new users who registered after this date
        :return: list of user_ids
        """

        user_ids = set()
        with self.session_scope() as session:
            for q in session.query(AuthorInfo).filter(AuthorInfo.last_sent < get_date()).all():
                user_ids.add(q.id)

        r = requests.get(self._config.get('API_VAULT_MYADS_USERS') % since.isoformat(),
                         headers={'Accept': 'application/json',
                                  'Authorization': 'Bearer {0}'.format(self._config.get('API_TOKEN'))}
                         )

        if r.status_code != 200:
            self.logger.warning('Error getting new myADS users from API')
        else:
            new_users = r.json()['users']
            for n in new_users:
                author = AuthorInfo(id=n, created=get_date(), last_sent=None)
                with self.session_scope() as session:
                    session.add(author)
                    session.commit()
                user_ids.add(n)

        return list(user_ids)

    def get_recent_results(self, user_id=None, qid=None, input_results=None, ndays=7):
        """
        Compares input results to those in storage and returns only new results.
        Results newer than ndays old are automatically included in the result.
        Stores new results.

        :param qid: int; ID of the query
        :param input_results: list; all results from a given query, as returned from solr
        :param ndays: int; number of days to automatically consider results new

        :return: list; new results
        """

        now = get_date()
        ndays_date = now - timedelta(days=ndays)
        old_results = set()
        with self.session_scope() as session:
            # get stored results older than ndays old
            q = session.query(Results).filter(and_(Results.qid == qid,
                                                   Results.user_id == user_id,
                                                   Results.created < ndays_date)).all()

            for res in q:
                old_results.update(res.results)

            # remove results older than ndays old - this is returned
            output_results = set(input_results).difference(old_results)

            # now remove the rest of the stored results, so we're not storing any overlap
            r = session.query(Results).filter(and_(Results.qid == qid,
                                                   Results.user_id == user_id,
                                                   Results.created >= ndays_date))

            for res in r.all():
                old_results.update(res.results)

            # remove all results currently stored for this query - this is stored
            new_results = list(output_results.difference(old_results))

            results = Results(user_id=user_id, qid=qid, results=new_results, created=now)
            session.add(results)
            session.commit()

        # note that old results will be returned if the bibcode has changed; it's a feature not a bug
        return list(output_results)
