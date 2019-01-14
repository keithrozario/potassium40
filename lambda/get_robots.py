import logging
import io
import json
import os
import boto3
import requests
import lambda_multiproc
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# There must be a logger called main_logger
logger = logging.getLogger('main_logger')
level = logging.INFO
logger.setLevel(level)

headers = {'User-Agent': 'p40Bot'}


def request(rows, conn):

    """
    Receives a list of text to be processed, one element per row
    Returns a list of dictionaries to be combined into a single file
    """
    s = requests.session()
    s.headers.update(headers)
    responses = []

    for row in rows:
        # get domain name from row of majestic top 1 million
        url = '{}'.format(row.split(',')[2].strip())

        try:
            response = s.get("http://{}/robots.txt".format(url),
                             verify=False,
                             timeout=1.5)

            if response.status_code == 200 and response.url[-10:] == 'robots.txt':
                if 'user-agent:' in response.text.lower():
                    responses.append({'domain': url,
                                      'robots.txt': response.text})

        except requests.exceptions.RequestException:
            pass
        except UnicodeError:  # sometimes occur with websites
            pass

    conn.send(responses)
    conn.close()


def get_robots(event, context):

    """
    Wrapper around init_requests, set the name of the file to read here.
    """

    event['file_name'] = 'random_majestic_million.csv'
    event['function'] = request  # pass the function

    results = lambda_multiproc.init_requests(event)

    logger.info("{} results returned".format(len(results)))
    logger.info("Requests complete, creating result file")

    # create file_obj in memory, must be in Binary form and implement read()
    file_obj = io.BytesIO(json.dumps(results).encode('utf-8'))

    # Upload file to bucket
    s3_client = boto3.client('s3')
    file_name = "{}-{}.{}".format(event['start_pos'], event['end_pos'], 'txt')
    logger.debug("Uploading to bucket:{}".format(os.environ['bucket_name']))
    s3_client.upload_fileobj(file_obj, os.environ['bucket_name'], file_name)  # bucket name in env var

    return {"status": 200,
            "file_name": event['file_name']
            }
