from .models import AuthorInfo
from adsputils import get_date

import requests
from flask import current_app

def get_users(app, since='1971-01-01T12:00:00Z'):
    """
    Checks internal storage and vault for all existing and new/updated myADS users. Adds new users to authors table (last_sent should be blank)
    :param app:
    :param since: used to fetch new users who registered after this date
    :return: list of user_ids
    """

    with current_app.session_scope() as session:
        q = session.query(AuthorInfo).filter(last_sent < get_date()).all()
        user_ids = set(q.id)

    r = requests.get(app.conf.get('API_VAULT_MYADS_SETUP') % since,
                     headers={'Accept': 'application/json',
                              'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))}
                     )
    if r.status_code != 200:
        logger.warning('Error getting new myADS users from API')
    else:
        new_users = r.json()['users']
        for n in new_users:
            author = AuthorInfo(id=n, created=get_date(), last_sent=None)
            with current_app.session_scope() as session:
                session.add(author)
                session.commit()
            user_ids.add(n)

    return list(user_ids)

def get_recent_results(app, input_results=None, ndays=7):
    """
    Compares input results to those in storage and returns only new results.
    Results newer than ndays old are automatically included in the result.

    :param input_results: dict; all results from a given query, as returned from solr
    :param ndays: int; number of days to automatically consider results new

    :return: dict; new
    """

def send_email(email_addr='', email_template=Email, payload=None):
    """
    Encrypts a payload using itsDangerous.TimeSerializer, adding it along with a base
    URL to an email template. Sends an email with this data using the current app's
    'mail' extension.
    :param email_addr:
    :type email_addr: basestring
    :param email_template: emails.Email
    :param payload
    :return: msg,token
    :rtype flask.ext.mail.Message, basestring
    """
    if payload is None:
        payload = []
    if isinstance(payload, (list, tuple)):
        payload = ' '.join(map(unicode, payload))
    msg = Message(subject=email_template.subject,
                  recipients=[email_addr],
                  body=email_template.msg_plain.format(payload=payload),
                  html=email_template.msg_html.format(payload=payload.replace('\n', '<br>'),
                                                      email_address=email_addr))

    try:
        current_app.extensions['mail'].send(msg)
    except:
        current_app.logger.warning('Error sending email to {0} with payload: {1}'.format(msg.recipients, msg.body))
        return None

    current_app.logger.info('Email sent to {0} with payload: {1}'.format(msg.recipients, msg.body))
    return msg

def get_user_email(userid=None):
    """
    Fetches user email address from adsws

    :param userid: str, system user ID

    :return: user email address
    """

    if userid:
        r = requests.get(app.conf.get(API_ADSWS_USER_EMAIL) % userid)
        if r.status_code == 200:
            return r.json()['email']
        else:
            current_app.logger.warning('Error getting user with ID {0} from the API'.format(userid))
            return None
    else:
        current_app.logger.error('No user ID supplied to fetch email')
        return None