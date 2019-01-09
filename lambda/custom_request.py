import logging
import requests
import lambda_multiproc
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


headers = {'User-Agent': 'p40Bot'}
logger = logging.getLogger()
level = logging.INFO
logger.setLevel(level)


def request(rows, conn):

    """
    Receives a list of text to be processed, one element per row
    Returns a list of dictionaries to be combined into a single file
    """
    s = requests.session()
    s.headers.update(headers)
    responses = []

    for row in rows:
        url = '{}'.format(row.split(',')[2].strip())

        try:
            response = s.get("http://{}/robots.txt".format(url),
                             verify=False,
                             timeout=2)

            if response.status_code == 200 and response.url[-10:] == 'robots.txt':
                if 'user-agent:' in response.text.lower():
                    responses.append({'domain': url,
                                      'robots.txt': response.text})
                else:
                    responses.append({'domain': url,
                                     'error': 'malformed'})
            else:
                responses.append({'domain': url,
                                  'status': response.status_code})
        except:
            responses.append({'domain': url,
                              'error': 'n.a'})

    conn.send(responses)
    conn.close()


def get_robots(event, context):

    """
    Wrapper around init_requests, set the name of the file to read here.
    """

    event['file_name'] = 'random_majestic_million.csv'

    lambda_multiproc.init_requests(event)

    return {"status": 200,
            "file_name": event['file_name']
            }
