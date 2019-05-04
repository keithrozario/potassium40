import os
import logging
import tempfile

import boto3


def main(event, context):
    """
    WARNINIG: Function doesn't work yet
    Uses S3 Select to convert a csv file (output from Athena)
    Into a jsonl file
    returns key of that file
    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    bucket_name = os.environ['bucket_name']
    file_name = event['result_file']
    return_file = f'{file_name[:-4]}.json'
    client = boto3.client('s3')

    response = client.select_object_content(Bucket=bucket_name,
                                            Key=file_name,
                                            Expression='select * from s3object s',
                                            ExpressionType='SQL',
                                            InputSerialization={'CSV': {'FileHeaderInfo': 'Use',
                                                                        'FieldDelimiter': ',',
                                                                        'RecordDelimiter': '\n',
                                                                        'QuoteCharacter': '"',
                                                                        'AllowQuotedRecordDelimiter': True}},
                                            OutputSerialization={'JSON': {'RecordDelimiter': '\n'}})
    records = ''
    for event in response['Payload']:
        if 'Records' in event:
            #  Undocumented S3 Select issue where QuotedDelimiters are replaced with \uffff
            records += (event['Records']['Payload'].decode('utf-8')).replace('\uffff', '\n')

    with tempfile.TemporaryFile() as file_in_mem:
        file_in_mem.write(records.encode('utf-8'))
        file_in_mem.seek(0)
        client.upload_fileobj(file_in_mem, bucket_name, return_file)

    return return_file
