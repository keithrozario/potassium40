import bcrypt
import logging


def check_hash(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    hash = event['hash'].encode('utf-8')

    for password in event.get('passwords', []):
        if bcrypt.checkpw(password.encode('utf-8'), hash):
            logger.info("HIT: password found at {}".format(password))

    return {'status': 200}
