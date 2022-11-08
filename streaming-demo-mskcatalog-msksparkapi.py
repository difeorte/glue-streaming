import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Kafka Stream from catalog
dataframe_KafkaStreamwithcatalog_node1 = glueContext.create_data_frame.from_catalog(
    database="gluestreamingdb",
    table_name="msk_locations_table",
    additional_options={
        "bootstrap.servers": "<your-MSK-bootstrap-string>",
        "startingOffsets": "earliest",
        "inferSchema": "false",
    },
    transformation_ctx="dataframe_KafkaStreamwithcatalog_node1",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:

        data_frame.show()
        data_frame.selectExpr("CAST(customerid AS STRING)", "CAST(latitude AS STRING) as value") \
            .write.format("kafka") \
            .option("kafka.bootstrap.servers", "<your-MSK-bootstrap-string>") \
            .option("topic", "test") \
            .save()

glueContext.forEachBatch(
    frame=dataframe_KafkaStreamwithcatalog_node1,
    batch_function=processBatch,
    options={
        "windowSize": "5 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
