import os
import json
import requests
import lxml.html
import logging.config

import creditinfo_scraper

from neo4j import GraphDatabase
from configparser import ConfigParser


def init_db(driver):
    """
    Creates indexes on two properties of a company: Register code (rc) and Name (name),
    and on one property of a person: Name(name). If indexes already exist, nothing happens.

    :param driver: an active connection to the database
    :return: none
    """
    def add_index(tx, *args, **kwargs):
        tx.run('CREATE INDEX ON :{0}({1})'.format(kwargs['label'], kwargs['property']))

    with driver.session() as session:
        session.write_transaction(add_index, label='Company', property='rc')
        session.write_transaction(add_index, label='Company', property='name')
        session.write_transaction(add_index, label='Person', property='name')


def main():
    english_version_url = 'https://www.e-krediidiinfo.ee/keel/en'

    # Load credentials
    with open(secrets_path) as fd:
        secrets = json.load(fd)

    # Create session for requests
    session = requests.Session()

    # Authenticate
    rs_secrets = secrets['resource']
    response = session.post(rs_secrets['url'], data=rs_secrets)

    # Verify that authentication succeeded
    tree = lxml.html.fromstring(response.text)

    if tree.xpath("//div[contains(@class, 'alert')]"):
        raise Exception('Invalid authentication credentials provided')

    logger.info('Logged in to https://www.e-krediidiinfo.ee')

    # Switch to english version
    session.get(english_version_url)

    # Create connection to the graph database
    db_secrets = secrets['database']
    driver = GraphDatabase\
        .driver(db_secrets['url'], auth=(db_secrets['login'], db_secrets['password']))

    logger.info('Connected to Neo4j')

    # Create indexes on frequently queried properties
    init_db(driver)

    # Create an instance of the scraper
    rc_range = range(rc_range_start, rc_range_end)
    scarper = creditinfo_scraper\
        .CreditinfoScarper(session, driver, rc_range=rc_range)

    logger.info('Created an instance of CreditinfoScarper; Processing...')

    # Start the scarper
    scarper.scrape()

    logger.info('Successfully collected the information about companies '
                'within (Register code) {0}'.format(rc_range))


if __name__ == '__main__':
    conf = ConfigParser()
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    conf.read(os.path.join(project_dir, 'scarper.conf'))

    secrets_path = conf.get('paths', 'secrets.json')
    logging_path = conf.get('paths', 'logging.conf')
    rc_range_start = int(conf.get('rc_range', 'start'))
    rc_range_end = int(conf.get('rc_range', 'end'))

    logging.config.fileConfig(logging_path)
    logger = logging.getLogger(__name__)

    main()
