import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689833116415 = glueContext.create_dynamic_frame.from_catalog(
    database="de-yt-clean",
    table_name="clean_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1689833116415",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1689833143940 = glueContext.create_dynamic_frame.from_catalog(
    database="de-yt-clean",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1689833143940",
)

# Script generated for node Join
Join_node1689833207786 = Join.apply(
    frame1=AWSGlueDataCatalog_node1689833143940,
    frame2=AWSGlueDataCatalog_node1689833116415,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1689833207786",
)

# Script generated for node Amazon S3
AmazonS3_node1689833592662 = glueContext.getSink(
    path="s3://de-yt-analytics-data",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1689833592662",
)
AmazonS3_node1689833592662.setCatalogInfo(
    catalogDatabase="de-yt-analytics-db", catalogTableName="de-yt-analytics_table"
)
AmazonS3_node1689833592662.setFormat("glueparquet")
AmazonS3_node1689833592662.writeFrame(Join_node1689833207786)
job.commit()
