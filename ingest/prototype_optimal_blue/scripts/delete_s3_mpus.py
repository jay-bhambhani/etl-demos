import os
import sys
import web
from logging import basicConfig, getLogger, INFO
from argparse import ArgumentParser
from helpers.s3_factory import S3Factory

parser = ArgumentParser(description='deletes idle multipart uploads to s3')
parser.add_argument('--bucket', help="name of bucket to delete from", type=str)

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)


def delete(bucket_name):
    
    s3_factory = S3Factory()
    s3_factory.s3_connect()
    s3_connection = s3_factory.connection

    try:
        s3_factory.delete_multipart_uploads(bucket_name)

    finally:
        s3_factory.connection.close()


if __name__ == '__main__':
    args = parser.parse_args()
    bucket_name = args.bucket
    delete(bucket_name)

