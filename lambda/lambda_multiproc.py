import math
import io
import json
import boto3
import os
import custom_request
import logging
from multiprocessing import Process, Pipe

logger = logging.getLogger()
level = logging.DEBUG
logger.setLevel(level)


def multiproc_requests(rows, proc_count):
    logger.info('Spawning {} processes'.format(proc_count))

    per_proc = int(math.ceil(len(rows) / proc_count))

    # create a list to keep all processes
    processes = []

    # create a list to keep connections
    parent_connections = []

    # create a process per instance
    for count in range(proc_count):
        # create a pipe for communication
        parent_conn, child_conn = Pipe()
        parent_connections.append(parent_conn)

        # create the process, pass instance and connection
        sub_list = [x for x in rows[count * per_proc: (count + 1) * per_proc]]
        process = Process(target=custom_request.request, args=(sub_list, child_conn,))
        processes.append(process)

    logger.info("Making HTTP Requests for {} rows".format(len(rows)))
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


def init_requests(event):

    logger.info("Starting...")
    # File is either provided in event['file_name'] or defaults to random_top-1m.csv
    file = "/opt/{}".format(event.get('file_name', 'random_top-1m.csv'))
    logger.debug("Retrieving rows from {}".format(file))

    rows = []
    if 'end_pos' in event and 'start_pos' in event:
        logger.debug("Opening {}".format(file))

        with open(file, 'r', encoding='utf-8') as f:
            rows = f.readlines()[event['start_pos']:event['end_pos']]

        logger.debug("Processing {} rows from file".format(len(rows)))
    else:
        logger.error("Error in arguments, start_pos and end_pos not found!!")
        exit(1)

    proc_count = event.get('proc_count', 125)
    logger.info("Requesting {} rows from {} to {} with {} procs".format(len(rows),
                                                                        rows[0],
                                                                        rows[-1],
                                                                        proc_count))
    results = multiproc_requests(rows, proc_count)
    logger.debug("{}".format(results))
    logger.info("Requests complete, creating result file")

    # create file_obj in memory, must be in Binary form and implement read()
    file_obj = io.BytesIO(json.dumps(results).encode('utf-8'))

    # Upload file to bucket
    s3_client = boto3.client('s3')
    file_name = "{}-{}.{}".format(event['start_pos'], event['end_pos'], 'txt')
    logger.debug("Uploading to bucket:{}".format(os.environ['bucket_name']))
    s3_client.upload_fileobj(file_obj, os.environ['bucket_name'], file_name)  # bucket name in env var

    return {'status': 200,
            'result': file_name}
