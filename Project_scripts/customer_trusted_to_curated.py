import sys
import awsglue.transforms import *
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

# script generated for node customer trusted

CustomerTrusted_node1682526335040 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-s3/customer/trusted"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1693320268024",
)

# Script generated for node Accelerometer landing
AccelerometerLanding_node1682526334822 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-s3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1693320269201",
)

# Script generated for node customer privacy filter
CustomerPrivacyFilter_node1682516328129 = Join.apply(
    frame1=CustomerTrusted_node1682526335040,
    frame2=AccelerometerLanding_node1682526334822,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacyFilter_node1682516328129",
)

# Script generated for node drop fields
DropFields_node1682517812631 = ApplyMapping.apply(
    frame=CustomerPrivacyFilter_node1682516328129,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="DropFields_node1682517812631",
)

# Script generated for node customer curated
CustomerCurated_node1 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1682517812631,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://stedi-s3/customer/curated/", "partitionKeys": []},
    transformation_ctx="CustomerCurated_node1",
)

job.commit()
