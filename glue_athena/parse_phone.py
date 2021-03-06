import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from phone_iso3166.country import phone_country
import pycountry

def parse_country_from_number(phone_number):
    """
    Parse country code from e.164 phone number
    TODO replace with https://pypi.org/project/phone-iso3166/
    """
    try:
        c = pycountry.countries.get(alpha_2=phone_country(phone_number))
        return c.name
    except Exception as e:
        return 'Unknown'

def parse_status(deliver_message):
    if deliver_message.find('been accepted') > 0:
        return 'Success'
    else:
        return 'Fail'

glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "sampledb"
tbl_name = "sms_dau_reports"

# S3 location for output
output_dir = "s3://cur-reports-liyx/formatted"

# Read data into a DynamicFrame using the Data Catalog metadata
sms_dur_dyf = glueContext.create_dynamic_frame.from_catalog(database = db_name, table_name = tbl_name)


# udf used to convert phone number to country code
parse_country_udf = udf(parse_country_from_number, StringType())
parse_status_udf = udf(parse_status, StringType())

sms_dur_df = sms_dur_dyf.toDF()

sms_dur_df = sms_dur_df.withColumn("country_code", parse_country_udf(sms_dur_df["destinationphonenumber"])).withColumn("status", parse_status_udf(sms_dur_df["deliverystatus"]))

# Turn it back to a dynamic frame
sms_dur_tmp = DynamicFrame.fromDF(sms_dur_df, glueContext, "nested")

# Rename, cast, and nest with apply_mapping
sms_dur_nest = sms_dur_tmp.apply_mapping([("publishtimeutc", "string", "publish_time_utc", "string"),
                                          ("messageid", "string",
                                           "message_id", "string"),
                                          ("destinationphonenumber", "string",
                                           "destination_phone_number", "string"),
                                          ("messagetype", "string",
                                           "message_type", "string"),
                                          ("deliverystatus", "string",
                                           "delivery_status", "string"),
                                          ("priceinusd", "double",
                                           "price_in_usd", "double"),
                                          ("partnumber", "long",
                                           "part_number", "long"),
                                          ("totalparts", "long", "total_parts", "long"),
                                          ("country_code", "string","country_code", "string"),
                                          ("status", "string","status", "string")])

# Write it out in Parquet
glueContext.write_dynamic_frame.from_options(frame = sms_dur_nest, connection_type = "s3", connection_options = {"path": output_dir}, format = "csv")
