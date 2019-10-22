from adsputils import get_date, setup_logging, load_config
from .emails import Email

import requests
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import urllib
import json
from jinja2 import Environment, PackageLoader, select_autoescape

logger = setup_logging('myads_utils')
config = {}
config.update(load_config())

env = Environment(
    loader=PackageLoader('myadsp', 'templates'),
    autoescape=select_autoescape(enabled_extensions=('html', 'xml'),
                                 default_for_string=True)
)


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


def get_query_results(myADSsetup=None):
    """
    Retrieves results for a stored query
    :param myADSsetup: dict containing query ID and metadata
    :return: payload: list of dicts containing query name, query url, raw search results
    """

    q = requests.get(config.get('API_VAULT_EXECUTE_QUERY') %
                     (myADSsetup['qid'], myADSsetup['fields'], myADSsetup['rows']),
                     headers={'Accept': 'application/json',
                              'Authorization': 'Bearer {0}'.format(config.get('API_TOKEN'))})
    if q.status_code == 200:
        docs = json.loads(q.text)['response']['docs']
        q_params = json.loads(q.text)['responseHeader']['params']
    else:
        logger.error('Failed getting results for QID {0}'.format(myADSsetup['qid']))
        docs = []
        q_params = None

    if q_params:
        # bigquery
        if q_params.get('fq', None) == u'{!bitset}':
            query_url = config.get('BIGQUERY_ENDPOINT') % myADSsetup['qid']
        # regular query
        else:
            urlparams = {'q': q_params.get('q', None), 'sort': q_params.get('sort', 'bibcode+desc')}
            query_url = config.get('QUERY_ENDPOINT') % urllib.urlencode(urlparams)
    else:
        # no parameters returned - should this url be something else?
        query_url = config.get('UI_ENDPOINT')

    return [{'name': myADSsetup['name'], 'query_url': query_url, 'results': docs}]


def get_template_query_results(myADSsetup=None):
    """
    Retrieves results for a templated query
    :param myADSsetup: dict containing query terms and metadata
    :return: payload: list of dicts containing query name, query url, raw search results
    """
    q = []
    sort = []
    if myADSsetup['template'] in ['arxiv', 'citations', 'authors']:
        name = [myADSsetup['name']]
    else:
        name = []
    if myADSsetup['template'] == 'arxiv':
        if type(myADSsetup['classes']) != list:
            tmp = [myADSsetup['classes']]
        else:
            tmp = myADSsetup['classes']
        classes = ' OR '.join(['arxiv_class:' + x for x in tmp])
        keywords = myADSsetup['data']
        q.append('bibstem:arxiv (({0}) OR ({1})) entdate:["NOW-2DAYS" TO NOW]'.format(classes, keywords))
        sort.append('score desc')
    elif myADSsetup['template'] == 'citations':
        keywords = myADSsetup['data']
        q.append('citations({0})'.format(keywords))
        sort.append('date desc')
    elif myADSsetup['template'] == 'authors':
        keywords = myADSsetup['data']
        q.append('{0} entdate:["NOW-25DAYS" TO NOW]'.format(keywords))
        sort.append('score desc')
    elif myADSsetup['template'] == 'keyword':
        keywords = myADSsetup['data']
        raw_name = myADSsetup['name']
        # most recent
        q.append('{0} entdate:["NOW-25DAYS" TO NOW]'.format(keywords))
        sort.append('entdate desc')
        name.append('{0} - Recent Papers'.format(raw_name))
        # most popular
        q.append('trending({0})'.format(keywords))
        sort.append('score desc')
        name.append('{0} - Most Popular'.format(raw_name))
        # most cited
        q.append('useful({0})'.format(keywords))
        sort.append('score desc')
        name.append('{0} - Most Cited'.format(raw_name))

    payload = []
    for i in range(len(q)):
        query = '{endpoint}?q={query}&sort={sort}'. \
                         format(endpoint=config.get('API_SOLR_QUERY_ENDPOINT'),
                                query=urllib.quote_plus(q[i]),
                                sort=urllib.quote_plus(sort[i]))
        r = requests.get('{query_url}&fl={fields}&rows={rows}'.
                         format(query_url=query,
                                fields=myADSsetup['fields'],
                                rows=myADSsetup['rows']),
                         headers={'Authorization': 'Bearer {0}'.format(config.get('API_TOKEN'))})

        if r.status_code != 200:
            logger.error('Failed getting results for query {0}'.format(q[i]))
            docs = []
        else:
            docs = json.loads(r.text)['response']['docs']
        payload.append({'name': name[i], 'query_url': query, 'results': docs})

    return payload


def _get_first_author_formatted(result_dict=None, author_field='author_norm'):
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


def _get_title(result_dict=None):
    """
    Get the title
    :param result_dict:
    :return: formatted title
    """

    if type(result_dict.get('title', '')) == list:
        title = result_dict.get('title')[0]
    else:
        title = result_dict.get('title', '')

    return title


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

env.globals['_get_first_author_formatted'] = _get_first_author_formatted
env.globals['_get_title'] = _get_title


def payload_to_html(payload=None, col=1, frequency='daily'):
    """
    Converts the myADS results into the HTML formatted message payload
    :param payload: list of dicts
    :param col: number of columns to display in formatted email (1 or 2)
    :param frequency: 'daily' or 'weekly' notification
    :return: HTML formatted payload
    """

    date_formatted = get_date().strftime("%B %d, %Y")

    if col == 1:
        template = env.get_template('one_col.html')
        return template.render(frequency=frequency,
                               date=date_formatted,
                               payload=payload,
                               abs_url=config.get('ABSTRACT_UI_ENDPOINT'))

    elif col == 2:
        left_col = payload[:len(payload) // 2]
        right_col = payload[len(payload) // 2:]
        template = env.get_template('two_col.html')
        return template.render(frequency=frequency,
                               date=date_formatted,
                               left_payload=left_col,
                               right_payload=right_col,
                               abs_url=config.get('ABSTRACT_UI_ENDPOINT'))

    else:
        logger.warning('Incorrect number of columns (col={0}) passed for payload {1}. No formatting done'.
                       format(col, payload))
        return None
