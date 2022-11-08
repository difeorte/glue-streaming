# AWS Glue Streaming examples with both native and Spark Structured Streaming S3 and MSK sources and sinks

Pyspark scripts in AWS Glue streaming to cover the following use cases:

1. Read data fed in real time from an Amazon S3 path using Spark Structured Streaming file store source API, process it, write the results to an MSK topic. The script for this case is "streaming-demo-s3sparksapi-msksparksapi".

2. Read data from a topic in MSK using the AWS Glue Data Catalog, process it, and then write the results to an MSK topic using the Spark Structured Streaming Kafka sink API. The script for this case is "streaming-demo-mskcatalog-msksparkapi".

3. Read data from a topic in MSK using the AWS Glue Data Catalog, process it, and then write the results to an Amazon S3 sink. The script for this case is "glue-streaming-demo".
