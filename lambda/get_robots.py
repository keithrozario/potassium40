import logging
import io
import json
import os
import boto3
import urllib3
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
        url = 'http://{}/robots.txt'.format(row.split(',')[2].strip())

        try:
            response = s.get(url,
                             verify=False,
                             timeout=1.5)
            if response.status_code == 200 and response.url[-10:] == 'robots.txt':
                if 'user-agent:' in response.text.lower():
                    if len(response.content) < 1024 * 1024:
                        try:
                            responses.append({'domain': url,
                                              'robots.txt': response.content.decode('utf-8')})
                        except UnicodeDecodeError:
                            logger.error(f"Robots.txt for {url} is not properly encoded")
                    else:
                        logger.error(f"Robots.txt for {url} is larger than 1 MB")

        except requests.exceptions.RequestException:
            logger.error(f"Request Exception for {url}")
        except UnicodeError:  # sometimes occur with websites
            pass
        except urllib3.exceptions.HeaderParsingError:
            logger.error(f"Failed Header parsing for {url}")

    conn.send(responses)
    conn.close()


def get_robots(event, context):

    """
    Wrapper around init_requests, set the name of the file to read here.
    """

    s3_prefix = 'robots/'

    try:
        message = json.loads(event['Records'][0]['body'])
        logger.info(message)
    except (json.JSONDecodeError, KeyError):
        logger.error("JSON Decoder error for event: {}".format(event))
        return {'status': 500}

    message['file_name'] = 'majestic_million.csv'
    message['function'] = request  # pass the function

    results = lambda_multiproc.init_requests(message)
    logger.debug("{} results returned".format(len(results)))
    logger.debug("Requests complete, creating result file")

    # create file_obj in memory, must be in Binary form and implement read()
    with io.BytesIO() as file_obj:
        for result in results:
            file_obj.write(json.dumps(result).encode('utf-8'))
            file_obj.write('\n'.encode('utf-8'))
        file_obj.seek(0)  # set to beginning of stream
        # Upload file to bucket
        s3_client = boto3.client('s3')
        file_name = "{}-{}.{}".format(message['start_pos'], message['end_pos'], 'txt')
        logger.debug("Uploading to bucket:{}".format(os.environ['bucket_name']))
        s3_client.upload_fileobj(file_obj, os.environ['bucket_name'], s3_prefix + file_name)

    return {"status": 200,
            "file_name": message['file_name']
            }
