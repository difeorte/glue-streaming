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

schema = "`no_alliance` INT, `description_alliance` STRING, `latitude` STRING, `longitude` STRING, `expiry_date` STRING"

s3DF = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", "10") \
    .load("s3://<your-bucket>/data/")
    
def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        data_frame.show()
        data_frame.selectExpr("CAST(no_alliance AS STRING)", "CAST(description_alliance AS STRING) as value") \
            .write.format("kafka") \
            .option("kafka.bootstrap.servers", "<your-msk-bootstrap-string>") \
            .option("topic", "test") \
            .save()

glueContext.forEachBatch(
    frame=s3DF,
    batch_function=processBatch,
    options={
        "windowSize": "5 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
