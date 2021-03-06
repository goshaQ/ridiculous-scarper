import time
import neo4j
import logging
import requests

from lxml import html
from multiprocessing.pool import ThreadPool

logger = logging.getLogger(__name__)


class CreditinfoScarper:
    def __init__(self, session, driver, search_params=None):
        """
        Initialization.

        :param session: an active connection to the resource
        :param driver: an active connection to the database
        :param search_params: a dictionary with search parameters (default: None)
        """
        super().__init__()
        logger.debug(
            'Created new CreditinfoScarper object with params: '
            f'session={session}, driver={driver}, search_params={search_params}')

        self._VAT_URL = 'https://www.e-krediidiinfo.ee/isik/{0}/vat'
        self._SEARCH_URL = 'https://www.e-krediidiinfo.ee/otsing'
        self._SEARCH_PARAMS = {
            'riik': 'et',  # Search from (default: Estonia)
            'q': ''  # The search query (default: None)
        }

        self._PROP_FORMATTED = {
            'Business name':            'name',
            'Register code':            'rc',
            'Operating address':        'op_address',
            'Legal address':            'l_address',
            'VAT No':                   'vat_no',
            'Founded':                  'founded',
            'Capital':                  'capital',
            'Phone':                    'phone',
            'E-mail':                   'email',
            'Representatives':          'repr',
            'Main activity':            'activity',
            'Taxes paid':               'taxes',
            'The number of employees':  'empl_num',
            'VAT Liable Income':        'vat_income'
        }

        if isinstance(search_params, dict) \
                and all(key in self._SEARCH_PARAMS.keys() for key in search_params.keys()):
            self._SEARCH_PARAMS.update(search_params)

        if self._SEARCH_PARAMS['riik'] == 'et':
            pass
        elif self._SEARCH_PARAMS['riik'] == 'fi':
            logger.error('At the moment, Finish companies are not supported')
            raise NotImplementedError('Search of Finish companies not implemented')
        elif self._SEARCH_PARAMS['riik'] == 'lt':
            logger.error('At the moment, Estonian companies are not supported')
            raise NotImplementedError('Search of Latvian companies not implemented')
        else:
            logger.error('The specified country is not known')
            raise ValueError(f'Illegal country code: {self._SEARCH_PARAMS["riik"]}')

        if isinstance(session, requests.Session):
            self._SESSION = session
        else:
            logger.error('Invalid session object')
            raise ValueError('Invalid session object')

        if isinstance(driver, neo4j.DirectDriver):
            self._DRIVER = driver
        else:
            logger.error('Invalid driver object')
            raise ValueError('Invalid driver object')

    def _get_request(self, search_url, params, timeout=3.):
        """
        Wrapper that handles exceptions of the GET request.

        :param search_url: the search query url
        :param params: the search parameters
        :param timeout: a limit on the server response time
        :return: the server response on the search query
        """

        try:
            response = self._SESSION.get(search_url, params=params, timeout=timeout)
        except requests.Timeout as err:
            logger.error(f'Connection timeout: {err}')
            return None
        except requests.ConnectionError as err:
            logger.error(f'Network problem occurred: {err}')
            return None

        if not response.ok:
            logger.warning(f'Register code: {params["q"]}; HTTP Error: {response.status_code}')
            return None

        return response

    def _process_query_response(self, tree):
        """
        Processes the response to the search GET request from the server. More
        precisely, extracts from the tree information about a company and puts
        it ine dictionary.

        :param tree: lxml HTML tree obtained after the search query
        :return: a dictionary containing information about a company
        """
        company_info = {}

        for row in tree.xpath("//table[@class='table-company-info']//tr"):
            prop = [text.strip() for text in row.xpath("td//text()") if text.strip()]
            if prop:
                key = prop[0].replace(':', '')

                if key == 'VAT No':
                    response = self._get_request(
                        self._VAT_URL.format(company_info['rc']), None)

                    if response is not None and response.text != 'Is not VAT payer':
                        val = response.text[response.text.find('(') + 1:response.text.find(')')]
                    else:
                        val = 'null'
                elif key == 'Founded':
                    val = '-'.join(prop[1].split('/')[::-1])
                elif key == 'Main activity':
                    val = prop[1][:prop[1].find('\n')]
                elif key == 'Representatives':
                    val = prop[1:-1]
                elif key == 'Taxes paid':
                    if 'No information' not in prop[1]:
                        val = '; '.join(prop[2:])
                    else:
                        val = 'null'
                elif key == 'The number of employees':
                    if 'No information' not in prop[1]:
                        val = prop[1][prop[1].find(': ') + 1:prop[1].find(' (')]
                    else:
                        val = 'null'
                elif key == 'VAT Liable Income':
                    if 'No information' not in prop[1]:
                        val = prop[1][:prop[1].find(' (')]
                    else:
                        val = 'null'
                elif key in self._PROP_FORMATTED:
                    val = prop[1]
                else:
                    continue

                company_info[self._PROP_FORMATTED[key]] = val

        return company_info

    def _store_company_info(self, company_info):
        """
        Stores information about a company into the graph database.

        :param company_info: a dictionary containing information about a company
        :return: none
        """
        def add_node_company(tx, *args, **kwargs):
            props = ', '.join('c.' + prop + ' = $' + prop for prop in kwargs.keys())
            tx.run('MERGE (c: Company {{rc: $rc}}) '
                   f'ON CREATE SET {props}', *args, **kwargs)

        def add_node_person(tx, *args, **kwargs):
            props = ', '.join('(p: Person {name: "' + name + '"}' + ')' for name in kwargs['p_name'])
            tx.run(f'MERGE {props}', *args, **kwargs)

        def add_rela_works_in(tx, *args, **kwargs):
            tx.run('MATCH (c: Company {name: $c_name}), (p: Person) '
                   'WHERE p.name in $p_name '
                   'MERGE (p)-[:WORKS_IN]->(c)', *args, **kwargs)

        with self._DRIVER.session() as session:
            representatives = company_info.pop('repr')
            session.write_transaction(add_node_company, **company_info)
            session.write_transaction(add_node_person, p_name=representatives)
            session.write_transaction(add_rela_works_in, p_name=representatives, c_name=company_info['name'])

    def _search(self, rc):
        """
        The actual search of information about a company. The result
        is stored in the graph database.

        :return: none
        """
        logger.info(f'Register code: {rc}; Making a search request;')

        response = self._get_request(self._SEARCH_URL, {**self._SEARCH_PARAMS, 'q': rc})
        if response is None:
            return

        tree = html.fromstring(response.text)
        if tree.xpath("//div[contains(@class, 'alert') and not(contains(@style, 'display:none;'))]"
                      "or //p[text()='Company is deleted']"):
            logger.warning(f'Non-existent Register code: {rc}')
            return

        logger.info(f'Register code: {rc}; Extracting information about a company;')

        company_info = self._process_query_response(tree)

        logger.info(f'Register code: {rc}; Storing information in the database;')

        self._store_company_info(company_info)

    def scrape(self, rc_range, req_per_sec=1.5, num_threads=4):
        """
        Makes multiple different search requests in parallel.

        :param rc_range: range of registration codes
        :param req_per_sec: the number of requests per second (default: 1.5)
        :param num_threads: the number of threads that  (default: 4)
        :return: none
        """

        if isinstance(rc_range, range) \
                and 10000000 <= rc_range.start and rc_range.stop <= 99999999:
            pass
        else:
            logger.error('The range of registration codes must be within [10000000, 99999999]')
            raise ValueError(f'Illegal range of registration codes: {rc_range}')

        if isinstance(num_threads, int) \
                and 1 <= num_threads <= 16:
            thread_pool = ThreadPool(num_threads)
        else:
            logger.error('The number of threads must be within [1, 16]')
            raise ValueError(f'Invalid number of threads: {num_threads}')

        if isinstance(req_per_sec, (int, float)) \
                and 0 < req_per_sec:
            sleep_time = 1 / req_per_sec
        else:
            logger.error('The number of requests per second must be positive')
            raise ValueError(f'Invalid number of requests per second: {req_per_sec}')

        logger.info('Started extracting the information')

        for rc in rc_range:
            thread_pool.apply_async(self._search, (rc,))
            time.sleep(sleep_time)

        thread_pool.close()
        thread_pool.join()

        logger.info('Finished extracting the information')
