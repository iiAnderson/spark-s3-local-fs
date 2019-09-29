# Spark S3 Local Context

This allows running Spark jobs with a S3 filesystem (Instead of HDFS) locally, something which is not possible with standard spark implementations.

This repository includes a simple LocalContext class, which wrapps a standard Spark configuration, providing some extra configuration to allow S3 Local access.

NOTE: This seems very dependent on versions of Spark, so implementation success may vary.