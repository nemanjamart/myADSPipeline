from adsputils import get_date, ADSCelery
from .models import AuthorInfo, Results

from datetime import timedelta
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import exc as ormexc


class myADSCelery(ADSCelery):

    def get_users(self, since='1971-01-01T12:00:00Z'):
        """
        Checks internal storage and vault for all existing and new/updated myADS users. Adds new users to authors table (last_sent should be blank)
        :param since: used to fetch new users who registered after this date
        :return: list of user_ids
        """

        user_ids = set()
        with self.session_scope() as session:
            for q in session.query(AuthorInfo).filter(AuthorInfo.last_sent < get_date()).all():
                user_ids.add(q.id)

        r = self.client.get(self._config.get('API_VAULT_MYADS_USERS') % get_date(since).isoformat(),
                            headers={'Accept': 'application/json',
                                     'Authorization': 'Bearer {0}'.format(self._config.get('API_TOKEN'))}
                            )

        if r.status_code != 200:
            self.logger.warning('Error getting new myADS users from API')
        else:
            new_users = r.json()['users']
            for n in new_users:
                try:
                    q = session.query(AuthorInfo).filter_by(id=n).one()
                except ormexc.NoResultFound:
                    author = AuthorInfo(id=n, created=get_date(), last_sent=None)
                    with self.session_scope() as session:
                        session.add(author)
                        session.commit()
                user_ids.add(n)

        return list(user_ids)

    def get_recent_results(self, user_id=None, qid=None, setup_id=None, input_results=None, ndays=7):
        """
        Compares input results to those in storage and returns only new results.
        Results newer than ndays old are automatically included in the result.
        Stores new results.

        :param user_id: int; ADSWS user ID
        :param qid: string; QID of the query (from vault "queries" table)
        :param setup_id: int; ID from myADSsetup field (from vault myADS export); used for templated queries
        :param input_results: list; all results from a given query, as returned from solr
        :param ndays: int; number of days to automatically consider results new

        :return: list; new results
        """

        if not qid and not setup_id:
            self.logger.warning('Must pass either qid or setup ID to get recent results. User: {0}'.format(user_id))
            return None

        now = get_date()
        ndays_date = now - timedelta(days=ndays)
        old_results = set()
        with self.session_scope() as session:
            # get stored results older than ndays old
            if qid:
                q = session.query(Results).filter(and_(Results.qid == qid,
                                                       Results.user_id == user_id,
                                                       Results.created < ndays_date)).all()
            else:
                q = session.query(Results).filter(and_(Results.setup_id == setup_id,
                                                       Results.user_id == user_id,
                                                       Results.created < ndays_date)).all()

            for res in q:
                old_results.update(res.results)

            # remove results older than ndays old - this is returned
            output_results = set(input_results).difference(old_results)

            # now remove the rest of the stored results, so we're not storing any overlap
            if qid:
                r = session.query(Results).filter(and_(Results.qid == qid,
                                                       Results.user_id == user_id,
                                                       Results.created >= ndays_date))
            else:
                r = session.query(Results).filter(and_(Results.setup_id == setup_id,
                                                       Results.user_id == user_id,
                                                       Results.created >= ndays_date))

            for res in r.all():
                old_results.update(res.results)

            # remove all results currently stored for this query - this is stored
            new_results = list(output_results.difference(old_results))

            # don't store empty results sets
            if new_results:
                if qid:
                    results = Results(user_id=user_id, qid=qid, results=new_results, created=now)
                else:
                    results = Results(user_id=user_id, setup_id=setup_id, results=new_results, created=now)

                session.add(results)
                session.commit()

        # note that old results will be returned if the bibcode has changed; it's a feature not a bug
        return list(output_results)
