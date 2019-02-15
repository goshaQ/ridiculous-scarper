import json
import requests

from neo4j import GraphDatabase

import authenticator
import creditinfo_scraper

authentication_url = 'https://www.e-krediidiinfo.ee/auth/regular/login'

# Load credentials
with open('../secrets.json') as fd:
    secrets = json.load(fd)

# Create session
session = authenticator.authenticated_session(authentication_url, secrets['resource'])

# Create connection to the graph database
db_secrets = secrets['database']
driver = GraphDatabase.driver(db_secrets['url'], auth=(db_secrets['login'], db_secrets['password']))

rc_range = range(14209942, 14211043)
scarper = creditinfo_scraper.CreditinfoScarper(session, driver, rc_range)
scarper.scrape()
