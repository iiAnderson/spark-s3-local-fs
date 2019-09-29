import sys
import boto3
from datetime import datetime

from spark_s3_local import LocalContext

from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from awsglue.context import GlueContext

for arg in sys.argv:
    if arg == "--local" or arg == "--l":
        is_local = True

'''

    A simple example of using the Spark Local context Class, where if the --local parameter is
    passed the S3 local context will be setup - so testing can be completed without running a cluster.

'''

class FileBatcher():

    def __init__(self, bucket_name, repartition_number, is_local=False):

        if is_local:
            sc = LocalContext("local[*]")
        else:
            glueContext = GlueContext(SparkContext.getOrCreate())
            sc = glueContext.spark_session  
            pass

        self.sql_context = SQLContext(sc)
        self.s3_bucket = bucket_name
        self.repartition_number = repartition_number

        s3_resource = boto3.resource('s3')
        self.bucket_resource = s3_resource.Bucket(bucket_name)

    def read_and_batch_dataframe(self, path):
        
        df = self.sql_context \
            .read.json("s3a://" + self.s3_bucket + "/" + path + "*.json")

        return df.coalesce(self.repartition_number)

    def write_dataframe(self, path, df):

        df.write.mode("append")\
            .format('json')\
            .option('compression', 'none')\
            .save("s3a://" + self.s3_bucket + "/" + path)

        self.bucket_resource.objects.filter(Prefix=path).delete()