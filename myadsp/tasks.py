
import adsputils
from myadsp import app as app_module
from myadsp import utils
from .models import AuthorInfo
from .emails import myADSTemplate

#from flask import current_app
from kombu import Queue
import os
import json
import datetime
from sqlalchemy.orm import exc as ormexc

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
app = app_module.myADSCelery('myADS-pipeline', proj_home=proj_home, local_config=globals().get('local_config', {}))
logger = app.logger

app.conf.CELERY_QUEUES = (
    Queue('process', app.exchange, routing_key='process'),
)

# ============================= TASKS ============================================= #

@app.task(queue='process')
def task_process_myads(message):
    """
    Process the myADS notifications for a given user

    :param message: contains the message inside the packet
        {
         'userid': adsws user ID,
         'frequency': 'daily' or 'weekly',
         'force': Boolean (if present, we'll reprocess myADS notifications for the user,
            even if they were already processed today)
         'test_send_to': email address to send output to, if not that of the user (for testing)
         'retries': number of retries attempted
        }
    :return: no return
    """

    if 'userid' not in message:
        logger.error('No user ID received for {0}'.format(message))
        return
    if 'frequency' not in message:
        logger.error('No frequency received for {0}'.format(message))
        return

    userid = message['userid']
    with app.session_scope() as session:
        try:
            q = session.query(AuthorInfo).filter_by(id=userid).one()
            if message['frequency'] == 'daily':
                last_sent = q.last_sent_daily
            else:
                last_sent = q.last_sent_weekly
        except ormexc.NoResultFound:
            author = AuthorInfo(id=userid, created=adsputils.get_date(), last_sent_daily=None, last_sent_weekly=None)
            session.add(author)
            session.flush()
            last_sent = None
            session.commit()
        if last_sent and last_sent.date() == adsputils.get_date().date():
            # already sent email today
            if not message['force']:
                logger.warning('Email for user {0} already sent today'.format(userid))
                return
            else:
                logger.info('Email for user {0} already sent today, but force mode is on'.format(userid))

    # first fetch the myADS setup from /vault/get-myads
    if last_sent:
        # the start date should be one day after the last sent date, so the results don't overlap
        start_date = last_sent + datetime.timedelta(days=1)
        r = app.client.get(app.conf.get('API_VAULT_MYADS_SETUP_DATE') % (userid, start_date),
                           headers={'Accept': 'application/json',
                                    'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})
    else:
        r = app.client.get(app.conf.get('API_VAULT_MYADS_SETUP') % userid,
                           headers={'Accept': 'application/json',
                                    'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})

    if r.status_code != 200:
        if message.get('retries', None):
            retries = message['retries']
        else:
            retries = 0
        if retries < app.conf.get('TOTAL_RETRIES', 3):
            message['retries'] = retries + 1
            task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
            logger.warning('Failed getting myADS setup for {0}; will try again later. Retry {1}'.format(userid, retries))
            return
        else:
            logger.warning('Maximum number of retries attempted for {0}. myADS processing failed.'.format(userid))
            return

    if message.get('test_bibcode', None):
        # check that the solr searcher we're getting is still ok by querying for the test bibcode
        q = app.client.get('{0}?q=identifier:{1}&fl=bibcode,identifier,entry_date'.format(app.conf.get('API_SOLR_QUERY_ENDPOINT'),
                                                                                          message.get('test_bibcode')),
                           headers={'Authorization': 'Bearer ' + app.conf.get('API_TOKEN')})

        fail = True
        if q.status_code != 200:
            logger.warning('Error retrieving the test bibcode {0} from solr while processing for user {1}. Retrying'.
                           format(message.get('test_bibcode'), userid))
        elif q.json()['response']['numFound'] == 0:
            logger.warning('Test bibcode {0} not found in solr while processing for user {1}. Retrying'.
                           format(message.get('test_bibcode'), userid))
        else:
            fail = False

        if fail:
            if message.get('solr_retries', None):
                retries = message['solr_retries']
            else:
                retries = 0
            if retries < app.conf.get('TOTAL_RETRIES', 3):
                message['solr_retries'] = retries + 1
                task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_SOLR_RESEND_WINDOW', 3600))
                logger.warning('Solr error occurred while processing myADS email for user {0}; rerunning. Retry {1}'.
                               format(userid, retries))
                return
            else:
                logger.warning('Maximum number of retries attempted for {0}. myADS processing failed: '
                               'solr searchers were not updated.'.format(userid))
                return

    # then execute each qid /vault/execute-query/qid
    setup = r.json()
    payload = []
    has_results = 0
    for s in setup:
        if s['frequency'] == message['frequency']:
            # only return 5 results, unless it's the daily arXiv posting, then return max
            # TODO should all stateful queries return all results or will this be overwhelming for some? well-cited
            # users can get 40+ new cites in one weekly astro update
            if s['frequency'] == 'daily':
                s['rows'] = app.conf.get('MAX_NUM_ROWS_DAILY', 2000)
            else:
                s['rows'] = app.conf.get('MAX_NUM_ROWS_WEEKLY', 5)
            s['fields'] = 'bibcode,title,author_norm,identifier,year,bibstem'
            if s['type'] == 'query':
                qtype = 'general'
            elif s['type'] == 'template':
                qtype = s['template']
            else:
                logger.warning('Wrong query type passed for query {0}, user {1}'.format(s, userid))
                continue

            try:
                raw_results = utils.get_template_query_results(s)
            except RuntimeError:
                if message.get('query_retries', None):
                    retries = message['query_retries']
                else:
                    retries = 0
                if retries < app.conf.get('TOTAL_RETRIES', 3):
                    message['query_retries'] = retries + 1
                    logger.warning('Error getting template query results for user {0}. Retrying. '
                                   'Retry:'.format(userid, retries))
                    task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
                    return
                else:
                    logger.warning('Maximum number of query retries attempted for user {0}; myADS processing '
                                   'failed due to retrieving query results failures.'.format(userid))
                    continue

            for r in raw_results:
                # for stateful queries, remove previously seen results, store new results
                if s['stateful']:
                    docs = r['results']
                    bibcodes = [doc['bibcode'] for doc in docs]
                    if s.get('qid', None):
                        good_bibc = app.get_recent_results(user_id=userid,
                                                           qid=s['qid'],
                                                           input_results=bibcodes,
                                                           ndays=app.conf.get('STATEFUL_RESULTS_DAYS', 7))
                    else:
                        good_bibc = app.get_recent_results(user_id=userid,
                                                           setup_id=s['id'],
                                                           input_results=bibcodes,
                                                           ndays=app.conf.get('STATEFUL_RESULTS_DAYS', 7))
                    results = [doc for doc in docs if doc['bibcode'] in good_bibc]
                else:
                    results = r['results']

                # keep track of queries that have returned results
                if results:
                    has_results += 1

                # even if a query doesn't have results, still include it in the email for completeness
                payload.append({'name': r['name'],
                                'query_url': r['query_url'],
                                'results': results,
                                'query': r['query'],
                                'qtype': qtype,
                                'id': s['id']})
        else:
            # wrong frequency for this round of processing
            continue

    # don't send the email if there are no matching queries or if all matching queries return no results
    if len(payload) == 0 or has_results == 0:
        logger.info('No payload for user {0} for the {1} email. No email was sent.'.format(userid, message['frequency']))
        return

    # if test email address provided, send there; otherwise fetch user email address
    if message.get('test_send_to', None):
        email = message.get('test_send_to')
    else:
        email = utils.get_user_email(userid=userid)

    if message['frequency'] == 'daily':
        subject = 'Daily myADS Notification'
    else:
        subject = 'Weekly myADS Notification'

    payload_plain = utils.payload_to_plain(payload)
    if len(payload) < app.conf.get('NUM_QUERIES_TWO_COL', 3):
        payload_html = utils.payload_to_html(payload, col=1, frequency=message['frequency'], email_address=email)
    else:
        payload_html = utils.payload_to_html(payload, col=2, frequency=message['frequency'], email_address=email)
    msg = utils.send_email(email_addr=email,
                           email_template=myADSTemplate,
                           payload_plain=payload_plain,
                           payload_html=payload_html,
                           subject=subject)

    if msg:
        # update author table w/ last sent datetime
        with app.session_scope() as session:
            q = session.query(AuthorInfo).filter_by(id=userid).one()
            if message['frequency'] == 'daily':
                q.last_sent_daily = adsputils.get_date()
            else:
                q.last_sent_weekly = adsputils.get_date()

            session.commit()

    else:
        if message.get('send_retries', None):
            retries = message['send_retries']
        else:
            retries = 0
        if retries < app.conf.get('TOTAL_RETRIES', 3):
            message['send_retries'] = retries + 1
            task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
            logger.warning('Error sending myADS email for user {0}, email {1}; rerunning. Retry {2}'.format(userid, email, retries))
            return
        else:
            logger.warning('Maximum number of retries attempted for {0}. myADS processing failed at sending the email.'.format(userid))
            return

