import math
import logging
from multiprocessing import Process, Pipe

# all functions that lambda_multiproc must create this logger
logger = logging.getLogger('main_logger')


def multiproc_requests(rows, proc_count, func):
    logger.debug('Spawning {} processes'.format(proc_count))

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
        process = Process(target=func, args=(sub_list, child_conn,))
        processes.append(process)

    logger.debug("Making Requests for {} rows".format(len(rows)))
    # start all processes
    for process in processes:
        process.start()

    logger.debug("Processes Started, waiting for closed connections")

    responses = []
    logger.debug("Reading info")
    for parent_connection in parent_connections:
        responses.extend(parent_connection.recv())

    for process in processes:
        process.join()

    return responses


def init_requests(event):

    """
    intializes multi processing of a file,
    processing each row from start_row to end_row,
    passing each row to function

    event['file_name'] = File Name to process, file must be in the /opt directory
    event['start_pos'] = start position (row number) of the file to begin process
    event['end_pos'] = end position (row number) of the file to stop processing
    event['function'] = function to process each row with
    event['proc_count] = number of multiple processes to use

    :return:
    """

    logger.debug("Starting...")
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
    results = multiproc_requests(rows, proc_count, event['function'])

    return results
