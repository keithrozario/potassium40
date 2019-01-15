import boto3
import json
import logging

logger = logging.getLogger()
level = logging.INFO
logger.setLevel(level)


def check_lambda(event, context):

    """
    Returns number of started and ended lambda functions in a log group
    """

    log_client = boto3.client('logs')

    log_group_name = '/aws/lambda/{}'.format(event['function_name'])
    start_time = event['start_time']

    filter_patterns = ['END RequestId']  # don't get the Started
    results = dict()

    for filter_pattern in filter_patterns:

        response = log_client.filter_log_events(logGroupName=log_group_name,
                                                filterPattern=filter_pattern,
                                                startTime=start_time)
        num_events = len(response.get('events', []))
        logger.info("Found {} events".format(num_events))

        # loop through finding all logs
        while response.get('nextToken', False):
            response = log_client.filter_log_events(logGroupName=log_group_name,
                                                    filterPattern=filter_pattern,
                                                    startTime=start_time,
                                                    nextToken=response['nextToken'])
            num_events += len(response.get('events', []))
            logger.info("Found {} events".format(num_events))

        logger.info("Done")
        results[filter_pattern] = num_events

    return {'status': 200,
            'results': json.dumps(results)}


if __name__ == '__main__':
    event = {
            "function_name": "potassium40-functions-get_robots",
            "start_time": 0
            }
    check_lambda(event, {})
