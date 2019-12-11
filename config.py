# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

SQLALCHEMY_URL = 'sqlite:///'
SQLALCHEMY_ECHO = False

# celery config
CELERY_INCLUDE = ['myadsp.tasks']
CELERY_BROKER = 'pyamqp://'

API_TOKEN = 'fix me'

UI_ENDPOINT = 'https://ui.adsabs.harvard.edu'
ABSTRACT_UI_ENDPOINT = UI_ENDPOINT + '/abs/%s/abstract'
BIGQUERY_ENDPOINT = UI_ENDPOINT + '/search/q=docs(%s)'
QUERY_ENDPOINT = UI_ENDPOINT + '/search/%s'

API_ENDPOINT = 'https://api.adsabs.harvard.edu'
API_SOLR_QUERY_ENDPOINT = API_ENDPOINT + '/v1/search/query/'
API_VAULT_MYADS_USERS = API_ENDPOINT + '/v1/vault/myads-users/%s'
API_VAULT_MYADS_SETUP = API_ENDPOINT + '/v1/vault/get-myads/%s'
API_VAULT_EXECUTE_QUERY = API_ENDPOINT + '/v1/vault/execute_query/%s?fl=%s&rows=%s'
API_ADSWS_USER_EMAIL = API_ENDPOINT + '/v1/user/%s'

# For stateful results, number of days after which we will consider a result stale and no longer show it
STATEFUL_RESULTS_DAYS = 7

# Reschedule sending if there's an error (units=seconds)
MYADS_RESEND_WINDOW = 60*10

# Number of days back, from today, to check for new arXiv records
ARXIV_TIMEDELTA_DAYS = 1

# Directories for incoming arXiv submissions
ARXIV_INCOMING_ABS_DIR = '/proj/ads/abstracts/sources/ArXiv'
ARXIV_UPDATE_AGENT_DIR = ARXIV_INCOMING_ABS_DIR + '/UpdateAgent'

MAIL_DEFAULT_SENDER = 'no-reply@adslabs.org'
MAIL_PASSWORD = "fix-me"
MAIL_PORT = 587
MAIL_SERVER = "fix-me"
MAIL_USERNAME = "fix-me"