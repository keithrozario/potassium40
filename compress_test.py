import boto3
import gzip
import logging
import csv
import json
import tempfile

client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('p40.123456')
obj = bucket.Object('example.csv')
event = {'result_file': 'example.csv'}

logger = logging.getLogger()
logger.setLevel(logging.INFO)


tmp = tempfile.SpooledTemporaryFile(mode='w+b')
logger.info('Downloading result file')
with tmp as fileobj:
    bucket.download_fileobj(event['result_file'], fileobj)
    logger.info('File Downloaded')
    fileobj.seek(0)
    logger.info('Decoding File to utf-8')
    data = (fileobj.read()).decode('utf-8')
    logger.info('File Decoded')

logger.info('Converting CSV to JSONL')
csv_rows = csv.reader(data.split('\r\n'), delimiter=',', quotechar='"')
jsons = [json.dumps({'domain': row[0], 'robots': row[1]}) for row in csv_rows]
logger.info('File Converted')

jsonl = '\n'.join(jsons[1:])

with gzip.open('output.jsonl.zip', mode='wb', compresslevel=2) as compressed_file:
    compressed_file.write(jsonl.encode('utf-8'))

