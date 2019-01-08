import math
import requests
import io
import json
import boto3
import os
import logging

from multiprocessing import Process, Pipe
from urllib.parse import urlparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning


logger = logging.getLogger()
level = logging.INFO
logger.setLevel(level)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
headers = {'User-Agent': 'p40Bot'}


def make_requests(urls, conn):
    logger.debug(urls)

    s = requests.session()
    s.headers.update(headers)
    responses = []
    for url in urls:

        try:
            response = s.get(url,
                             verify=False,
                             timeout=1.5)
            if response.status_code == 200 and response.url[-10:] == 'robots.txt':
                if 'user-agent:' in response.text.lower():
                    responses.append({'domain': urlparse(url).netloc,
                                      'robots.txt': response.text})
            # else:
            #     responses.append({'domain': urlparse(url).netloc,
            #                       'status_code': response.status_code})
        except:
            pass
            # logger.info('Exception occured for {}'.format(url))
            # responses.append({'domain': urlparse(url).netloc,
            #                   'error': 'timeout'})

    conn.send(responses)
    logger.debug("Sent: {}: ".format(urls))
    conn.close()


def requests_all(urls, proc_count):
    logger.info('Spawning {} processes'.format(proc_count))

    per_proc = int(math.ceil(len(urls) / proc_count))

    # create a list to keep all processes
    processes = []

    # create a list to keep connections
    parent_connections = []
    child_connections =[]

    # create a process per instance
    for count in range(proc_count):
        # create a pipe for communication
        parent_conn, child_conn = Pipe()
        parent_connections.append(parent_conn)

        # create the process, pass instance and connection
        sub_list = [x for x in urls[count * per_proc: (count + 1) * per_proc]]
        process = Process(target=make_requests, args=(sub_list, child_conn,))
        processes.append(process)

    logger.info("Making HTTP Requests for {} urls".format(len(urls)))
    # start all processes
    for process in processes:
        process.start()

    logger.info("Processes Started, waiting for closed connections")

    responses = []
    logger.info("Reading info")
    for parent_connection in parent_connections:
        responses.extend(parent_connection.recv())

    for process in processes:
        process.join()

    return responses


def get_robots(event, context):

    file = '/opt/random_top-1m.csv'
    urls = []

    logger.debug("Retrieving URLS")
    if event.get('urls', []):
        logger.debug("Processing {} urls in event ".format(len(urls)))
        urls = ['http://{}/robots.txt'.format(url) for url in event['urls']]
    elif 'end_pos' in event and 'start_pos' in event:
        logger.debug("Opening {}".format(file))

        with open(file, 'r', encoding='utf-8') as url_file:
            urls = ['http://{}/robots.txt'.format(url.split(',')[1].strip())
                    for url in url_file.readlines()[event['start_pos']:event['end_pos']]]

        logger.debug("Processing {} urls from file".format(len(urls)))
    else:
        logger.debug("Error in arguments")
        exit(1)

    proc_count = event.get('proc_count', 6)

    logger.info("Requesting {} urls from {} to {} with {} procs".format(len(urls),
                                                                        urls[0],
                                                                        urls[-1],
                                                                        proc_count))
    results = requests_all(urls, proc_count)
    logger.debug("{}".format(results))
    logger.info("Requests complete, creating result file")

    # create file_obj in memory, must be in Binary form and implement read()
    file_obj = io.BytesIO(json.dumps(results).encode('utf-8'))

    # Upload file
    s3_client = boto3.client('s3')
    file_name = "{}{}".format(urlparse(urls[0]).netloc, '.txt')
    logger.debug("Uploading to bucket:{}".format(os.environ['bucket_name']))
    s3_client.upload_fileobj(file_obj, os.environ['bucket_name'], file_name)  # bucket name in env var

    return {'status': 200,
            'result': file_name}


