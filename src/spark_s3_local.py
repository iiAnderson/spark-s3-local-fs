import sys
import os
from datetime import datetime

import boto3
import configparser
import pyspark
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, \
    PairDeserializer, AutoBatchedSerializer, NoOpSerializer
from pyspark.profiler import ProfilerCollector, BasicProfiler

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'

class LocalContext(pyspark.SparkContext):

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
                 environment=None, batchSize=0, serializer=PickleSerializer(), conf=None,
                 gateway=None, jsc=None, profiler_cls=BasicProfiler):

        pyspark.SparkContext.__init__(self, master=master, appName=appName, sparkHome=sparkHome, pyFiles=pyFiles,
                 environment=environment, batchSize=batchSize, serializer=serializer, conf=conf,
                 gateway=gateway, jsc=jsc, profiler_cls=profiler_cls)

        
        config = configparser.ConfigParser()
        config.read(os.path.expanduser("~/.aws/credentials"))

        aws_profile = 'default' # your AWS profile to use

        access_id = config.get(aws_profile, "aws_access_key_id") 
        access_key = config.get(aws_profile, "aws_secret_access_key") 

        hadoopConf = self._jsc.hadoopConfiguration()

        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoopConf.set("fs.s3a.awsAccessKeyId", access_id)
        hadoopConf.set("fs.s3a.awsSecretAccessKey", access_key)
        hadoopConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")