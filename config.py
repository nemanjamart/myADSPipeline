# possible values: WARN, INFO, DEBUG
LOGGING_LEVEL = 'DEBUG'

SQLALCHEMY_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/test_myadspipeline"
SQLALCHEMY_ECHO = False

# celery config
CELERY_INCLUDE = ['myadsp.tasks']
CELERY_BROKER = 'pyamqp://'

API_TOKEN = 'fix me'

UI_ENDPOINT = 'https://ui.adsabs.harvard.edu'
ABSTRACT_UI_ENDPOINT = UI_ENDPOINT + '/abs/{0}/abstract?utm_source=myads&utm_medium=email&utm_campaign=type:{1}&utm_term={2}&utm_content=rank:{3}'
BIGQUERY_ENDPOINT = UI_ENDPOINT + '/search/q=docs(%s)'
QUERY_ENDPOINT = UI_ENDPOINT + '/search/%s'

API_ENDPOINT = 'https://api.adsabs.harvard.edu'
API_SOLR_QUERY_ENDPOINT = API_ENDPOINT + '/v1/search/query/'
API_VAULT_MYADS_USERS = API_ENDPOINT + '/v1/vault/myads-users/%s'
API_VAULT_MYADS_SETUP = API_ENDPOINT + '/v1/vault/get-myads/%s'
API_VAULT_EXECUTE_QUERY = API_ENDPOINT + '/v1/vault/execute_query/%s?fl=%s&rows=%s&sort=%s'
API_ADSWS_USER_EMAIL = API_ENDPOINT + '/v1/user/%s'

ARXIV_URL = 'https://ui.adsabs.harvard.edu/link_gateway/{0}/EPRINT_HTML?utm_source=myads&utm_medium=email&utm_campaign=type:{1}&utm_term={2}&utm_content=rank:{3}'

# For stateful results, number of days after which we will consider a result stale and no longer show it
STATEFUL_RESULTS_DAYS = 7

# Number of queries to switch from one to two column email format
NUM_QUERIES_TWO_COL = 3
MAX_NUM_ROWS_DAILY = 2000
MAX_NUM_ROWS_WEEKLY = 5

# Reschedule sending if there's an error (units=seconds)
MYADS_RESEND_WINDOW = 60*10
# Reschedule sending if there's an error with Solr (units=seconds)
MYADS_SOLR_RESEND_WINDOW = 60*15
TOTAL_RETRIES = 3

# Number of days back, from today, to check for new records
ARXIV_TIMEDELTA_DAYS = 1
ASTRO_TIMEDELTA_DAYS = 3

# Directories for incoming arXiv submissions
ARXIV_INCOMING_ABS_DIR = '/proj/ads/abstracts/sources/ArXiv'
ARXIV_UPDATE_AGENT_DIR = ARXIV_INCOMING_ABS_DIR + '/UpdateAgent'

# Directory for incoming astronomy articles
ASTRO_INCOMING_DIR = '/proj/ads/abstracts/ast/index/current/'
ASTRO_SAMPLE_SIZE = 3

MAIL_DEFAULT_SENDER = 'ads@cfa.harvard.edu'
MAIL_PASSWORD = None
MAIL_PORT = 25
MAIL_SERVER = None
MAIL_USERNAME = None
