import sys
import time
import argparse
import logging
import traceback
import requests
import warnings
from requests.packages.urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

from adsputils import setup_logging, get_date
from myadsp import tasks, utils
from myadsp.models import KeyValue

app = tasks.app
logger = setup_logging('run.py')

def process_myads(since=None, user_ids=None, frequency='daily', **kwargs):
    """
    Processes myADS mailings

    :param since: check for new myADS users since this date
    :param user_ids: users to process claims for, else all users - list

    :return: no return
    """
    if user_ids:
        for u in user_ids:
            tasks.task_process_myads({'userid':u, 'frequency': frequency, 'force': True})
            logger.info('Done (just the supplied user IDs)')
            return

    logging.captureWarnings(True)

    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            if frequency=='daily':
                kv = session.query(KeyValue).filter_by(key='last.process.daily').first()
            else:
                kv = session.query(KeyValue).filter_by(key='last.process.weekly').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1971-01-01T12:00:00Z'

    users_since_date = get_date(since)
    logger.info('Processing {0} myADS queries since: {1}'.format(frequency, users_since_date.isoformat()))

    all_users = app.get_users(users_since_date.isoformat())
    last_process_date = get_date()

    for user in all_users:
        try:
            tasks.task_process_myads.delay({'userid': user, 'frequency': frequency, 'force': False})
        except:  # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', user
            tasks.task_process_myads.delay({'userid': user, 'frequency': frequency, 'force': False})

    with app.session_scope() as session:
        if frequency=='daily':
            kv = session.query(KeyValue).filter_by(key='last.process.daily').first()
        else:
            kv = session.query(KeyValue).filter_by(key='last.process.weekly').first()
        if kv is None:
            if frequency=='daily':
                kv = KeyValue(key='last.process.daily', value=last_process_date.isoformat())
            else:
                kv = KeyValue(key='last.process.weekly', value=last_process_date.isoformat())
            session.add(kv)
        else:
            kv.value = last_process_date.isoformat()
        session.commit()

    print 'Done'
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

    args = parser.parse_args()

    if args.user_ids:
        args.user_ids = [x.strip() for x in args.user_ids.split(',')]

    if args.daily_updates:
        process_myads(args.since_date, args.user_ids, frequency='daily')
    if args.weekly_updates:
        process_myads(args.since_date, args.user_ids, frequency='weekly')