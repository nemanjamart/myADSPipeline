API_TOKEN = 'fix me'

API_ENDPOINT = 'https://api.adsabs.harvard.edu'
API_VAULT_MYADS_SETUP = API_ENDPOINT + '/v1/vault/get-myads/%s'
API_VAULT_EXECUTE_QUERY = API_ENDPOINT + '/v1/vault/execute_query/%s'
API_ADSWS_USER_EMAIL = API_ENDPOINT + '/v1/user/%s'

# For stateful results, number of days after which we will consider a result stale and no longer show it
STATEFUL_RESULTS_DAYS = 7

# Reschedule sending if there's an error
MYADS_RESEND_WINDOW = 60*10