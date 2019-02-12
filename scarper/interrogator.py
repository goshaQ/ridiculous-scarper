import json
import requests

import authenticator

authentication_url = 'https://www.e-krediidiinfo.ee/auth/regular/login'

# Load credentials
with open('../secrets.json') as fd:
    secrets = json.load(fd)

# Create session
session = authenticator.authenticated_session(authentication_url, secrets)
