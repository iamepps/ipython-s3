import boto3
import json
import pickle
from IPython.core.magic import (register_line_magic, register_cell_magic,
                                register_line_cell_magic)
from tempfile import NamedTemporaryFile

@register_line_magic
def s3_read(line):
    
    def parse_s3_path(path):
        path = path.strip('s3://')
        path_split = path.split('/')
        bucket = path_split[0]
        key = '/'.join(path_split[1:])
        return bucket, key

    def connect_to_bucket(bucket):
        try:
            session = boto3.Session()
            s3 = session.resource('s3')
            bucket = s3.Bucket('tes-data-science')
            return bucket
        except:
            print("Failed to connect to bucket")

    bucket_name, file_path = parse_s3_path(line)
    bucket = connect_to_bucket(bucket_name)
    file_extension = file_path.split('.')[-1]

    if file_extension == 'json':
        print("Returned deserialised json")
        return json.loads(bucket.Object(file_path).get()['Body'].read())

    elif file_extension in ['pkl', 'pickle']:
        print("Returned unpickled file")
        return pickle.loads(self.bucket.Object(file_path).get()['Body'].read())
    
    elif file_extension in ['csv', 'tsv', 'txt', 'jsonl']:
        print("Returned text")
        return bucket.Object(file_path).get()['Body'].read().decode()
    
    else:
        print("Returned fileobject")
        fp = NamedTemporaryFile()
        bucket.Object(file_path).download_fileobj(fp)
        obj = open(fp.name, 'r')
        fp.close()
        return obj
