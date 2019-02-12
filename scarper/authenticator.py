import requests
import lxml.html


def _verify_authentication(response):
    tree = lxml.html.fromstring(response.text)

    if tree.xpath("//div[contains(@class, 'alert')]"):
        raise Exception('Invalid authentication credentials provided')


def authenticated_session(authentication_url, secrets):
    # Create session for requests
    session = requests.Session()

    # Authenticate
    response = session.post(authentication_url, data=secrets)

    # Verify that authentication succeeded
    _verify_authentication(response)
