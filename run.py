from adsputils import setup_logging, get_date, load_config
from myadsp import tasks, utils
from myadsp.models import KeyValue

import os
import time
import argparse
import logging
import warnings
import datetime
import gzip
import random
import json
try:
    from urllib.parse import quote_plus
except ImportError:
    from urlparse import quote_plus
from requests.packages.urllib3 import exceptions

# ============================= INITIALIZATION ==================================== #

proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

app = tasks.app

# =============================== FUNCTIONS ======================================= #

def _arxiv_ingest_complete(date=None, sleep_delay=60, sleep_timeout=7200):
    """
    Check if new arXiv records are in Solr - run before running myADS processing
    :param date: date to check arXiv records for; default is set by days-delta from today in config (times in local time)
    :param sleep_delay: number of seconds to sleep between retries
    :param sleep_timeout: number of seconds to retry in total before timing out completely
    :return: test bibcode or None
    """

    if not date:
        date = (datetime.datetime.today() - datetime.timedelta(days=config.get('ARXIV_TIMEDELTA_DAYS'))).strftime('%Y-%m-%d')
    else:
        date = get_date(date).strftime('%Y-%m-%d')

    arxiv_file = config.get('ARXIV_UPDATE_AGENT_DIR') + '/UpdateAgent.out.' + date + '.gz'

    arxiv_records = []
    try:
        with gzip.open(arxiv_file, 'r') as flist:
            for l in flist.readlines():
                # sample line: oai/arXiv.org/0706/2491 2018-06-13T01:00:29
                arxiv_records.append(l.split()[0])
    except IOError:
        logger.warning('arXiv ingest file not found. Exiting.')
        return None

    arxiv_records.sort()

    # get the highest numbered ID
    is_new = False
    while is_new is False:
        last_record = arxiv_records.pop()
        try:
            test_new = float(last_record.split('/')[-2])
            is_new = True
        except ValueError:
            continue

    # get most recent arXiv id to test ingest later
    last_id = '.'.join(last_record.split('/')[-2:])

    total_delay = 0
    while total_delay < sleep_timeout:
        total_delay += sleep_delay
        r = app.client.get('{0}?q=identifier:{1}&fl=bibcode,identifier,entry_date'.format(config.get('API_SOLR_QUERY_ENDPOINT'), last_id),
                           headers={'Authorization': 'Bearer ' + config.get('API_TOKEN')})
        if r.status_code != 200:
            time.sleep(sleep_delay)
            logger.error('Error retrieving record for {0} from Solr ({1} {2}), retrying'.
                         format(last_id, r.status_code, r.text))
            continue

        numfound = r.json()['response']['numFound']
        if numfound == 0:
            # nothing found, try again after a sleep
            time.sleep(sleep_delay)
            logger.info('arXiv ingest not complete (test arXiv id: {0}). Sleeping {1}s, for a total delay of {2}s.'
                        .format(last_id, sleep_delay, total_delay))
            continue
        if numfound > 1:
            # returning this as true for now, since technically something was found
            logger.error('Too many records returned for id {0}'.format(last_id))

        logger.info('Numfound: {0} for test id {1}. Response: {2}. URL: {3}'.format(numfound, last_id,
                                                                                         json.dumps(r.json()), r.url))

        # check number of bibcodes from ingest
        if get_date().weekday() == 0:
            start_date = (get_date() - datetime.timedelta(days=3)).date()
        else:
            start_date = (get_date() - datetime.timedelta(days=1)).date()
        beg_pubyear = (get_date() - datetime.timedelta(days=180)).year
        q = app.client.get('{0}?q={1}'.format(config.get('API_SOLR_QUERY_ENDPOINT'),
                                              quote_plus('bibstem:arxiv entdate:["{0}Z00:00" TO NOW] '
                                                                'pubdate:[{1}-00 TO *]'.format(start_date, beg_pubyear))),
                           headers={'Authorization': 'Bearer ' + config.get('API_TOKEN')})
        logger.info('Total number of arXiv bibcodes ingested: {}'.format(q.json()['response']['numFound']))

        return last_id

    logger.warning('arXiv ingest did not complete within the {0}s timeout limit. Exiting.'.format(sleep_timeout))

    return None


def _astro_ingest_complete(date=None, sleep_delay=60, sleep_timeout=7200):
    """
    Check if new astronomy records are in Solr; run before weekly processing
    :param date: check to check against astronomy bibcode list last updated date
    :param sleep_delay: number of seconds to sleep between retries
    :param sleep_timeout: number of seconds to retry in total before timing out completely
    :return: test bibcode or None
    """

    if not date:
        date = (datetime.datetime.today() - datetime.timedelta(days=config.get('ASTRO_TIMEDELTA_DAYS')))
    else:
        date = get_date(date)

    astro_file = config.get('ASTRO_INCOMING_DIR') + 'matches.input'

    # make sure file is present and check modified datestamp on file - should be recent (otherwise contains old data)
    try:
        mod_date = datetime.datetime.fromtimestamp(os.path.getmtime(astro_file))
    except OSError:
        mod_date = None

    # if the file is old or missing, sleep until the file is present and updated
    if not mod_date or mod_date < date:
        total_delay = 0
        while total_delay < sleep_timeout:
            total_delay += sleep_delay
            time.sleep(sleep_delay)
            try:
                mod_date = datetime.datetime.fromtimestamp(os.path.getmtime(astro_file))
            except OSError:
                mod_date = None
            if mod_date and mod_date > date:
                break
        else:
            # timeout reached before astronomy update completed
            logger.warning('Astronomy update did not complete within the {0}s timeout limit. Exiting.'.format(sleep_timeout))

            return None

    # make sure the ingest file exists and has enough bibcodes
    total_delay = 0
    while total_delay < sleep_timeout:
        astro_records = []
        try:
            with open(astro_file, 'r') as flist:
                for l in flist.readlines():
                    # sample line: 2019A&A...632A..94J     K58-37447
                    astro_records.append(l.split()[0])
        except IOError:
            time.sleep(sleep_delay)
            total_delay += sleep_delay
            logger.warning('Error opening astronomy ingest file. Sleeping {0}s, for a total delay of {1}s'.
                           format(sleep_delay, total_delay))
            continue

        if len(astro_records) < 10:
            time.sleep(sleep_delay)
            total_delay += sleep_delay
            logger.warning('Astronomy ingest file too small - ingest not complete. Sleeping {0}s, for a total delay of {1}s'.
                           format(sleep_delay, total_delay))
            continue
        else:
            break
    else:
        return None

    # get several randomly selected bibcodes, in case one had ingest issues
    sample = random.sample(astro_records, config.get('ASTRO_SAMPLE_SIZE'))

    # check that the astronomy records have made it into solr
    total_delay = 0
    while total_delay < sleep_timeout:
        num_sampled = 0
        for s in sample:
            num_sampled += 1
            r = app.client.get('{0}?q=identifier:{1}&fl=bibcode,identifier,entry_date'.format(config.get('API_SOLR_QUERY_ENDPOINT'), s),
                               headers={'Authorization': 'Bearer ' + config.get('API_TOKEN')})
            # if there's a solr error, sleep then move to the next bibcode
            if r.status_code != 200:
                time.sleep(sleep_delay)
                total_delay += sleep_delay
                logger.error('Error retrieving bibcode {0} from Solr ({1} {2}), sleeping {3}s, for a total delay of {4}s'.
                             format(s, r.status_code, r.text, sleep_delay, total_delay))
                continue

            numfound = r.json()['response']['numFound']
            if numfound == 0:
                # nothing found - if all bibcodes in the sample were tried, sleep then start the while loop again
                if num_sampled == config.get('ASTRO_SAMPLE_SIZE'):
                    time.sleep(sleep_delay)
                    total_delay += sleep_delay
                    logger.warning('Astronomy ingest not complete for all in sample (sample: {0}). Sleeping {1}s, for a total delay of {2}s.'
                                   .format(sample, sleep_delay, total_delay))
                # if we haven't tried the others in the same, try the rest
                else:
                    logger.info(
                        'Astronomy ingest not complete (test astro bibcode: {0}). Trying the next in the sample.'
                        .format(s))
                continue
            elif numfound > 1:
                # returning this as true for now, since technically something was found
                logger.error('Too many records returned for bibcode {0}'.format(s))

            logger.info('Numfound: {0} for test bibcode {1}. Response: {2}. URL: {3}'.format(numfound, s,
                                                                                             json.dumps(r.json()),
                                                                                             r.url))
            return s

    logger.warning('Astronomy ingest did not complete within the {0}s timeout limit. Exiting.'.format(sleep_timeout))

    return None


def process_myads(since=None, user_ids=None, user_emails=None, test_send_to=None, admin_email=None, force=False,
                  frequency='daily', test_bibcode=None, **kwargs):
    """
    Processes myADS mailings

    :param since: check for new myADS users since this date
    :param user_ids: users to process claims for, else all users - list (given as adsws IDs)
    :param user_emails: users to process claims for, else all users - list (given as email addresses)
    :param test_send_to: for testing; process a given user ID but send the output to this email address
    :param admin_email: if provided, email is sent to this address at beginning and end of processing (does not trigger
    for processing for individual users)
    :param force: if True, will force processing of emails even if sent for a given user already that day
    :param frequency: basestring; 'daily' or 'weekly'
    :param test_bibcode: bibcode to query to test if Solr searcher has been updated
    :return: no return
    """
    if user_ids:
        for u in user_ids:
            tasks.task_process_myads({'userid': u, 'frequency': frequency, 'force': True,
                                      'test_send_to': test_send_to, 'test_bibcode': test_bibcode})

        logger.info('Done (just the supplied user IDs)')
        return

    if user_emails:
        for u in user_emails:
            r = app.client.get(config.get('API_ADSWS_USER_EMAIL') % u,
                               headers={'Accept': 'application/json',
                                        'Authorization': 'Bearer {0}'.format(config.get('API_TOKEN'))}
                               )
            if r.status_code == 200:
                user_id = r.json()['id']
            else:
                logger.warning('Error getting user ID with email {0} from the API. Processing aborted for this user'.format(u))
                continue

            tasks.task_process_myads({'userid': user_id, 'frequency': frequency, 'force': True,
                                      'test_send_to': test_send_to, 'test_bibcode': test_bibcode})

        logger.info('Done (just the supplied user IDs)')
        return

    logging.captureWarnings(True)

    if admin_email:
        msg = utils.send_email(email_addr=admin_email,
                               payload_plain='Processing started for {}'.format(get_date()),
                               payload_html='Processing started for {}'.format(get_date()),
                               subject='myADS {0} processing has started'.format(frequency))

    # if since keyword not provided, since is set to timestamp of last processing
    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            if frequency == 'daily':
                kv = session.query(KeyValue).filter_by(key='last.process.daily').first()
            else:
                kv = session.query(KeyValue).filter_by(key='last.process.weekly').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1971-01-01T12:00:00Z'

    users_since_date = get_date(since)
    logger.info('Processing {0} myADS queries since: {1}'.format(frequency, users_since_date.isoformat()))

    last_process_date = get_date()
    all_users = app.get_users(users_since_date.isoformat(), frequency=frequency)

    for user in all_users:
        try:
            tasks.task_process_myads.delay({'userid': user, 'frequency': frequency, 'force': force,
                                            'test_bibcode': test_bibcode})
        except:  # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', user
            tasks.task_process_myads.delay({'userid': user, 'frequency': frequency, 'force': force,
                                            'test_bibcode': test_bibcode})

    # update last processed timestamp
    with app.session_scope() as session:
        if frequency == 'daily':
            kv = session.query(KeyValue).filter_by(key='last.process.daily').first()
        else:
            kv = session.query(KeyValue).filter_by(key='last.process.weekly').first()
        if kv is None:
            if frequency == 'daily':
                kv = KeyValue(key='last.process.daily', value=last_process_date.isoformat())
            else:
                kv = KeyValue(key='last.process.weekly', value=last_process_date.isoformat())
            session.add(kv)
        else:
            kv.value = last_process_date.isoformat()
        session.commit()

    print 'Done submitting {0} myADS processing tasks for {1} users.'.format(frequency, len(all_users))
    logger.info('Done submitting {0} myADS processing tasks for {1} users.'.format(frequency, len(all_users)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-s',
                        '--since',
                        dest='since_date',
                        action='store',
                        default=None,
                        help='Process all new/udpated myADS users since this date, plus existing myADS users')

    parser.add_argument('-u',
                        '--uid',
                        dest='user_ids',
                        action='store',
                        default=None,
                        help='Comma delimited list of user ids to run myADS notifications for')

    parser.add_argument('-e',
                        '--email_user',
                        dest='user_emails',
                        action='store',
                        default=None,
                        help='Comma delimited list of user emails to run myADS notifications for')

    parser.add_argument('-d',
                        '--daily',
                        dest='daily_update',
                        action='store_true',
                        help='Process daily arXiv myADS notifications')

    parser.add_argument('-w',
                        '--weekly',
                        dest='weekly_update',
                        action='store_true',
                        help='Process weekly myADS notifications')

    parser.add_argument('-t',
                        '--test_send_to',
                        dest='test_send_to',
                        action='store',
                        default=None,
                        help='For testing; process a given user ID but send output to this email address')

    parser.add_argument('-a',
                        '--admin_email',
                        dest='admin_email',
                        action='store',
                        default=None,
                        help='Send email to this address at beginning and end of processing')

    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='Force processing even if already ran today')

    parser.add_argument('--wait',
                       dest='wait_send',
                       action='store',
                       type=int,
                       default=0,
                       help='Wait these many seconds after ingest to allow SOLR searchers to be in sync')

    args = parser.parse_args()

    if args.user_ids:
        args.user_ids = [x.strip() for x in args.user_ids.split(',')]

    if args.user_emails:
        args.user_emails = [x.strip() for x in args.user_emails.split(',')]

    if args.daily_update:
        arxiv_complete = False
        try:
            arxiv_complete = _arxiv_ingest_complete(sleep_delay=300, sleep_timeout=36000)
        except Exception as e:
            logger.warning('arXiv ingest: code failed with an exception: {0}'.format(e))
        if arxiv_complete:
            logger.info('arxiv ingest: complete')
            if args.wait_send:
                logger.info('arxiv ingest: waiting {0} seconds for all SOLR searchers to sync data'.format(args.wait_send))
                time.sleep(args.wait_send)
            logger.info('arxiv ingest: starting processing')
            process_myads(args.since_date, args.user_ids, args.user_emails, args.test_send_to, args.admin_email, args.force,
                          frequency='daily', test_bibcode=arxiv_complete)
        else:
            logger.warning('arXiv ingest: failed.')
            if args.admin_email:
                msg = utils.send_email(email_addr=args.admin_email,
                                       payload_plain='Error in the arXiv ingest',
                                       payload_html='Error in the arXiv ingest',
                                       subject='arXiv ingest failed')
    if args.weekly_update:
        astro_complete = False
        try:
            astro_complete = _astro_ingest_complete(sleep_delay=300, sleep_timeout=36000)
        except Exception as e:
            logger.warning('astro ingest: code failed with an exception: {0}'.format(e))
        if astro_complete:
            logger.info('astro ingest: complete')
            if args.wait_send:
                logger.info('astro ingest: waiting {0} seconds for all SOLR searchers to sync data'.format(args.wait_send))
                time.sleep(args.wait_send)
            logger.info('astro ingest: starting processing now')
            process_myads(args.since_date, args.user_ids, args.user_emails, args.test_send_to, args.admin_email, args.force,
                          frequency='weekly', test_bibcode=astro_complete)
        else:
            logger.warning('astro ingest: failed.')
            if args.admin_email:
                msg = utils.send_email(email_addr=args.admin_email,
                                       payload_plain='Error in the astronomy ingest',
                                       payload_html='Error in the astronomy ingest',
                                       subject='Astronomy ingest failed')
