
import adsputils
from myadsp import app as app_module
from myadsp import utils
from .models import AuthorInfo
from .emails import myADSTemplate

#from flask import current_app
from kombu import Queue
import requests
import os
import json

app = app_module.myADSCelery('myADS-pipeline', proj_home=os.path.realpath(os.path.join(os.path.dirname(__file__), '../')))
app.conf.CELERY_QUEUES = (
    Queue('process', app.exchange, routing_key='process'),
)
logger = app.logger


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
        q = session.query(AuthorInfo).filter_by(id=userid).one()
        if q.last_sent and q.last_sent.date() == adsputils.get_date().date():
            # already sent email today
            if not message['force']:
                logger.warning('Email for user {0} already sent today'.format(userid))
                return
            else:
                logger.info('Email for user {0} already sent today, but force mode is on'.format(userid))

    # first fetch the myADS setup from /vault/get-myads
    r = requests.get(app.conf.get('API_VAULT_MYADS_SETUP') % userid,
                     headers={'Accept': 'application/json',
                              'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})

    if r.status_code != 200:
        task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
        logger.warning('Failed getting myADS setup for {0}; will try again later'.format(userid))
        return

    # then execute each qid /vault/execute-query/qid
    setup = r.json()
    payload = []
    fields = 'bibcode,title,author_norm'
    for s in setup:
        if s['frequency'] == message['frequency']:
            # only return 5 results, unless it's the daily arXiv posting, then return max
            # TODO should all stateful queries return all results or will this be overwhelming for some? well-cited users can get 40+ new cites in one weekly astro update
            if s['frequency'] == 'daily' and s['stateful'] is False:
                rows = 2000
            else:
                rows = 5
            # sort by entdate desc here (or filter w/ fq), or suggest sort order in setup?
            q = requests.get(app.conf.get('API_VAULT_EXECUTE_QUERY') % (s['qid'], fields, rows),
                             headers={'Accept': 'application/json',
                                      'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})
            if q.status_code == 200:
                docs = json.loads(q.text)['response']['docs']
                bibc = [doc['bibcode'] for doc in docs]
                q_params = json.loads(q.text)['responseHeader']['params']
            else:
                logger.error('Failed getting new results for {0} for user {1}'.format(s, userid))
                bibc = []

            # for stateful queries, remove previously seen results, store new results
            if s['stateful']:
                good_bibc = app.get_recent_results(user_id=userid,
                                                   qid=s['qid'],
                                                   input_results=bibc,
                                                   ndays=app.conf.get('STATEFUL_RESULTS_DAYS', 7))
                results = [doc for doc in docs if doc['bibcode'] in good_bibc]
            else:
                results = docs

            if q_params:
                # bigquery
                if q_params.get('fq', None) == u'{!bitset}':
                    query_url = app.conf.get('BIGQUERY_ENDPOINT') % s['qid']
                # regular query
                else:
                    query_url = app.conf.get('QUERY_ENDPOINT') % q_params['q']
            else:
                # no parameters returned - should this url be something else?
                query_url = app.conf.get('UI_ENDPOINT')

            # TODO email formatting
            payload.append({'name': s['name'], 'query_url': query_url, 'results': results})
        else:
            # wrong frequency for this round of processing
            pass

    email = utils.get_user_email(userid=userid)
    if message['frequency'] == 'daily':
        subject = 'Daily arXiv myADS Notification'
    else:
        subject = 'Weekly myADS Notification'

    payload_plain = utils.payload_to_plain(payload)
    if len(payload) <= 3:
        payload_html = utils.payload_to_html(payload, col=1)
    else:
        payload_html = utils.payload_to_html(payload, col=2)
    msg = utils.send_email(email_addr=email,
                           email_template=myADSTemplate,
                           payload_plain=payload_plain,
                           payload_html=payload_html,
                           subject=subject)

    if msg:
        # update author table w/ last sent datetime
        with app.session_scope() as session:
            q = session.query(AuthorInfo).filter_by(id=userid).one()
            q.last_sent = adsputils.get_date()

            session.commit()

    else:
        task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
        logger.warning('Error sending myADS email for user {0}, email {1}; rerunning'.format(userid, email))

