import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dataframe_KafkaStreamnocatalog_node1 = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options={
        "bootstrap.servers": "your-msk-bootstrap-string",
        "connectionName": "msk",
        "classification": "json",
        "startingOffsets": "earliest",
        "topicName": "customersLocation",
        "inferSchema": "true",
        "typeOfData": "kafka",
    },
    transformation_ctx="dataframe_KafkaStreamnocatalog_node1",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        KafkaStreamnocatalog_node1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node S3 bucket
        S3bucket_node3_path = (
            "s3://<your-target-bucket>/results"
            + "/ingest_year="
            + "{:0>4}".format(str(year))
            + "/ingest_month="
            + "{:0>2}".format(str(month))
            + "/ingest_day="
            + "{:0>2}".format(str(day))
            + "/ingest_hour="
            + "{:0>2}".format(str(hour))
            + "/"
        )
        S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
            frame=KafkaStreamnocatalog_node1,
            connection_type="s3",
            format="json",
            connection_options={"path": S3bucket_node3_path, "partitionKeys": []},
            transformation_ctx="S3bucket_node3",
        )


glueContext.forEachBatch(
    frame=dataframe_KafkaStreamnocatalog_node1,
    batch_function=processBatch,
    options={
        "windowSize": "5 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
