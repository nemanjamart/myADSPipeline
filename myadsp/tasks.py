
import adsputils
from myadsp import app as app_module
from myadsp import utils
from .models import AuthorInfo

from flask import current_app
from kombu import Queue
import datetime
import requests
import os

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
         'userid': '.....',
         'start': 'ISO8801 formatted date (optional), used for calculating deltas in stateful queries'
         'force': Boolean (if present, we'll not skip unchanged
             profile)
        }
    :return: no return
    """

    if 'userid' not in message:
        logger.error('No user ID received for {0}'.format(message))
        raise RuntimeError('Bad input supplied')

    message['start'] = adsputils.get_date()
    userid = message['userid']
    with current_app.session_scope() as session:
        q = session.query(AuthorInfo).filter_by(id=userid).one()
        if q.last_sent and q.last_sent.date() == adsputils.get_date():
            # already sent email today
            if not message['force']:
                raise RuntimeError('Email for user {0} already sent today'.format(userid))
            else:
                logger.info('Email for user {0} already sent today, but force mode is on'.format(userid))

    # first fetch the myADS setup from /vault/get-myads
    r = requests.get(app.conf.get('API_VAULT_MYADS_SETUP') % userid,
                     headers={'Accept': 'application/json',
                              'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})

    if r.status_code != 200:
        task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
        raise RuntimeError('Failed getting myADS setup for {0}'.format(userid))

    # then execute each qid /vault/execute-query/qid
    setup = r.json()
    payload = []
    for s in setup:
        q = requests.get(app.conf.get('API_VAULT_EXECUTE_QUERY') % s['qid'],
                         headers={'Accept': 'application/json',
                                  'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})
        if q.status_code == 200:
            q_results = json.loads(q.text)['response']['docs']
        else:
            logger.error('Failed getting new results for {0} for user {1}'.format(s, userid))
            q_results = []

        # for stateful queries (how do I know which are stateful?), remove previously seen results, store new results
        if s['stateful']:
            results = utils.get_recent_results(app, q_results, ndays=app.conf.get('STATEFUL_RESULTS_DAYS'))
        else:
            results = q_results

        # TODO email formatting
        payload.append('{0}: {1}'.format(s['name'], results))

    email = utils.get_user_email(userid=userid)
    msg = utils.send_email(email_addr=email,
                           email_template=PermissionsChangedEmail,
                           payload=payload)

    if msg:
        # update author table w/ last sent datetime
        with current_app.session_scope() as session:
            q = session.query(AuthorInfo).filter_by(id=userid).one()
            q.last_sent = adsputils.get_date()

            session.commit()

    else:
        task_process_myads.apply_async(args=(message,), countdown=app.conf.get('MYADS_RESEND_WINDOW', 3600))
        raise RuntimeError('Error sending myADS email for user {0}, email {1}; rerunning'.format(userid, email))
