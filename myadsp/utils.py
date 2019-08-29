from adsputils import get_date, setup_logging, load_config
from .emails import Email

import requests
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = setup_logging('myads_utils')
config = {}
config.update(load_config())


def send_email(email_addr='', email_template=Email, payload_plain=None, payload_html=None, subject=None):
    """
    Encrypts a payload using itsDangerous.TimeSerializer, adding it along with a base
    URL to an email template. Sends an email with this data using the current app's
    'mail' extension.
    :param email_addr: basestring
    :param email_template: emails.Email
    :param payload_plain: basestring
    :param payload_html: basestring (formatted HTML)
    :param subject: basestring
    :return: msg: MIMEMultipart
    """
    if (email_addr == '') or (email_addr is None):
        logger.warning('No email address passed for myADS notifications. Not sending email')
        return None
    if payload_plain is None and payload_html is None:
        logger.warning('No payload passed for {0} for myADS notifications. Not sending email'.format(email_addr))
        return None

    if subject is None:
        subject = email_template.subject

    # subtype=alternative means each part is equivalent; last attached part is the one to display, if possible
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.get('MAIL_DEFAULT_SENDER')
    msg["To"] = email_addr
    plain = MIMEText(email_template.msg_plain.format(payload=payload_plain), "plain")
    html = MIMEText(email_template.msg_html.format(payload=payload_html, email_address=email_addr), "html")
    msg.attach(plain)
    msg.attach(html)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(config.get('MAIL_SERVER'), config.get('MAIL_PORT'), context=context) as server:
            server.login(config.get('MAIL_USERNAME'),
                         config.get('MAIL_PASSWORD'))
            server.sendmail(config.get('MAIL_DEFAULT_SENDER'),
                            email_addr,
                            msg.as_string())
    except:
        logger.error('Error sending email to {0} with payload: {1}'.format(email_addr, plain))
        return None

    logger.info('Email sent to {0} with payload: {1}'.format(email_addr, plain))
    return msg


def get_user_email(userid=None):
    """
    Fetches user email address from adsws

    :param userid: str, system user ID

    :return: user email address
    """

    if userid:
        r = requests.get(config.get('API_ADSWS_USER_EMAIL') % userid)
        if r.status_code == 200:
            return r.json()['email']
        else:
            logger.warning('Error getting user with ID {0} from the API'.format(userid))
            return None
    else:
        logger.error('No user ID supplied to fetch email')
        return None


def _get_first_author_formatted(result_dict, author_field='author_norm'):
    """
    Get the first author, format it correctly
    :param result_dict: dict containing the results from solr for a single bibcode, including the author list
    :param author_field: Solr field to select first author from
    :return: formatted first author
    """

    if author_field not in result_dict:
        logger.warning('Author field {0} not supplied in result {1}'.format(author_field, result_dict))
        return ''

    authors = result_dict.get(author_field)
    if type(authors) == list:
        first_author = authors[0]
        num = len(authors)
    else:
        first_author = authors
        num = 1
    if num > 1:
        first_author += ',+:'

    return first_author


def payload_to_plain(payload=None):
    """
    Converts the myADS results into the plain text message payload
    :param payload: list of dicts
    :return: plain text formatted payload
    """
    formatted = ''
    for p in payload:
        formatted += "{0} ({1}) \n".format(p['name'], p['query_url'])
        for r in p['results']:
            first_author = _get_first_author_formatted(r)
            if type(r.get('title', '')) == list:
                title = r.get('title')[0]
            else:
                title = r.get('title', '')
            formatted += "{0}: {1} {2}\n".format(r['bibcode'], first_author, title)
        formatted += "\n"

    return formatted


def payload_to_html(payload=None, col=1):
    """
    Converts the myADS results into the HTML formatted message payload
    :param payload: list of dicts
    :param col: number of columns to display in formatted email (1 or 2)
    :return: HTML formatted payload
    """
    start_col = '''<td align="center" valign="top" width="{0}" class="templateColumnContainer">\n''' + \
                '''    <table border="0" cellpadding="10" cellspacing="0" width="100%">\n'''
    end_col = '''</table>\n</td>\n'''
    head = '<h3><a href="{0}" style="color: #1C459B; font-style: italic;font-weight: bold;">{1}</a></h3>'
    item = '<a href="{0}" style="color: #5081E9;font-weight: normal;text-decoration: underline;">{1}</a> {2} {3}<br>'

    if col == 1:
        formatted = start_col.format('100%')
        for p in payload:
            formatted += '''<tr>\n    <td valign="top" class="columnContent">\n    '''
            formatted += head.format(p['query_url'], p['name'])
            formatted += '\n    '
            for r in p['results']:
                bibc_url = config.get('ABSTRACT_UI_ENDPOINT') % r['bibcode']
                first_author = _get_first_author_formatted(r)
                if type(r.get('title', '')) == list:
                    title = r.get('title')[0]
                else:
                    title = r.get('title', '')
                formatted += item.format(bibc_url, r['bibcode'], first_author, title)
                formatted += '\n'
            formatted += '''</td>\n</tr>\n'''
        formatted += end_col
        return formatted

    elif col == 2:
        left_col = payload[:len(payload) // 2]
        right_col = payload[len(payload) // 2:]
        formatted = start_col.format('50%')
        for p in left_col:
            formatted += '''<tr>\n    <td valign="top" class="leftColumnContent">\n    '''
            formatted += head.format(p['query_url'], p['name'])
            formatted += '\n    '
            for r in p['results']:
                bibc_url = config.get('ABSTRACT_UI_ENDPOINT') % r['bibcode']
                first_author = _get_first_author_formatted(r)
                if type(r.get('title', '')) == list:
                    title = r.get('title')[0]
                else:
                    title = r.get('title', '')
                formatted += item.format(bibc_url, r['bibcode'], first_author, title)
                formatted += '\n'
            formatted += '''</td>\n</tr>\n'''
        formatted += end_col
        for p in right_col:
            formatted += '''<tr>\n    <td valign="top" class="rightColumnContent">\n    '''
            formatted += head.format(p['query_url'], p['name'])
            formatted += '\n    '
            for r in p['results']:
                bibc_url = config.get('ABSTRACT_UI_ENDPOINT') % r['bibcode']
                first_author = _get_first_author_formatted(r)
                if type(r.get('title', '')) == list:
                    title = r.get('title')[0]
                else:
                    title = r.get('title', '')
                formatted += item.format(bibc_url, r['bibcode'], first_author, title)
                formatted += '\n'
            formatted += '''</td>\n</tr>\n'''
        formatted += end_col
        return formatted

    else:
        logger.warning('Incorrect number of columns (col={0}) passed for payload {1}. No formatting done'.
                       format(col, payload))
        return None
