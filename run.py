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

def process_myads(since=None,user_ids=None, **kwargs):
    """
    Processes myADS mailings

    :param since: for stateful query results, new results since this time - RFC889 formatted string
    :param user_ids: users to process claims for, else all users - list

    :return: no return
    """
    if user_ids:
        for u in user_ids:
            tasks.task_process_myads({'userid':u, 'since': since})
            logger.info('Done (just the supplied user IDs)')
            return

    logging.captureWarnings(True)

    if not since or isinstance(since, basestring) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.process').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1971-01-01T12:00:00Z'

    from_date = get_date(since)
    logger.info('Processing myADS queries since: {0}'.format(from_date.isoformat()))

    all_users = utils.get_users(app, from_date.isoformat())
    from_date = get_date()

    for user in all_users:
        try:
            tasks.task_process_myads.delay({'userid': user, 'force': False})
        except:  # potential backpressure (we are too fast)
            time.sleep(2)
            print 'Conn problem, retrying...', user
            tasks.task_process_myads.delay({'userid': user, 'force': False})

    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.process').first()
        if kv is None:
            kv = KeyValue(key='last.process', value=from_date.isoformat())
            session.add(kv)
        else:
            kv.value = from_date.isoformat()
        session.commit()

    print 'Done'
    logger.info('Done submitting myADS processing tasks for {0} users.'.format(len(all_users)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-s',
                        '--since',
                        dest='since_date',
                        action='store',
                        default=None,
                        help='Starting date for reindexing')

    parser.add_argument('-u',
                        '--uid',
                        dest='user_ids',
                        action='store',
                        default=None,
                        help='Comma delimited list of user ids to run myADS notifications for')

    args = parser.parse_args()
    if args.user_ids:
        args.user_ids = [x.strip() for x in args.user_ids.split(',')]

    process_myads(args.since_date, args.user_ids)