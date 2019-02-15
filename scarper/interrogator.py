import json
import requests
import lxml.html
import logging.config

import creditinfo_scraper

from neo4j import GraphDatabase


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
    with open('../secrets.json') as fd:
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

    # Switch to english version
    session.get(english_version_url)

    # Create connection to the graph database
    db_secrets = secrets['database']
    driver = GraphDatabase\
        .driver(db_secrets['url'], auth=(db_secrets['login'], db_secrets['password']))

    # Create indexes on frequently queried properties
    init_db(driver)

    # Create an instance of the scraper
    scarper = creditinfo_scraper\
        .CreditinfoScarper(session, driver, rc_range=range(12886000, 12886100))

    # Start the scarper
    scarper.scrape()


if __name__ == '__main__':
    logging.config.fileConfig('../logging.conf')
    logger = logging.getLogger(__name__)

    main()
