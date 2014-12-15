import boto
import boto.s3.connection
import os
import json
import codecs

s3 = boto.connect_s3(aws_access_key_id=os.environ['AWS_ACCESS_KEY'], aws_secret_access_key=os.environ['AWS_SECRET_KEY'])
bucket = s3.get_bucket("twitter-stream-predictions")

with codecs.open("predictions-final", "w", "utf-8") as output:
    for key in bucket.list():
        key.get_contents_to_filename("predictions/" + key.name)
        for line in open("predictions/" + key.name, "r"):
            output.write(line.decode('utf8'))

