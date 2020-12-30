import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def parse_country_from_number(phone_number):
    """
    Parse country code from e.164 phone number
    TODO replace with https://pypi.org/project/phone-iso3166/
    """
    if phone_number.startswith('+1'):
        return 'US'
    elif phone_number.startswith('+20'):
        return 'EG'
    elif phone_number.startswith('+30'):
        return 'Greece'
    else:
        return 'Others'



glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "sampledb"
tbl_name = "sms_dau_reports"

# S3 location for output
output_dir = "s3://cur-reports-liyx/formatted"

# Read data into a DynamicFrame using the Data Catalog metadata
sms_dur_dyf = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)


# udf used to convert phone number to country code
parse_udf = udf(parse_country_from_number, StringType())
sms_dur_df = sms_dur_dyf.toDF()

sms_dur_df = sms_dur_df.withColumn("country_code", parse_udf(sms_dur_df["destinationphonenumber"]))

# Turn it back to a dynamic frame
sms_dur_tmp = DynamicFrame.fromDF(sms_dur_df, glueContext, "nested")

# Rename, cast, and nest with apply_mapping
sms_dur_nest = sms_dur_tmp.apply_mapping([("publishtimeutc", "string", "publishtimeutc", "string"),
                                          ("messageid", "string",
                                           "messageid", "string"),
                                          ("destinationphonenumber", "string",
                                           "destinationphonenumber", "string"),
                                          ("messagetype", "string",
                                           "messagetype", "string"),
                                          ("deliverystatus", "string",
                                           "deliverystatus", "string"),
                                          ("priceinusd", "double",
                                           "priceinusd", "double"),
                                          ("partnumber", "long",
                                           "partnumber", "long"),
                                          ("totalparts", "long", "totalparts", "long"),
                                          ("country_code", "string","country_code", "string")])

# Write it out in Parquet
glueContext.write_dynamic_frame.from_options(frame = sms_dur_nest, connection_type = "s3", connection_options = {"path": output_dir}, format = "csv")
