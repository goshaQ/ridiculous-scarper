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
    response = session.post(secrets['url'], data=secrets)

    # Verify that authentication succeeded
    _verify_authentication(response)

    english_version_url = 'https://www.e-krediidiinfo.ee/keel/en'

    # Switch to english version
    session.get(english_version_url)

    return session
