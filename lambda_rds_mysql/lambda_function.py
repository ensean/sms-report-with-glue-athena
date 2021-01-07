import json
import urllib.parse
import boto3
import pymysql.cursors
import logging
from phone_iso3166.country import phone_country
import pycountry

logger = logging.getLogger()

# Connect to the database
try:
    connection = pymysql.connect(host='sms-usage-report.czagvy5trygi.us-west-2.rds.amazonaws.com',
                                 user='username',
                                 password='password',
                                 db='sms_report',
                                 charset='utf8mb4',
                                 connect_timeout=10,
                                 cursorclass=pymysql.cursors.DictCursor)
except Exception as e:
    logger.error(e)
    raise e

s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(bucket)
    print(key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        save_content_to_db(connection, file_content)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def save_content_to_db(conn, file_content):
    rows = file_content.split('\n')
    with conn.cursor() as cursor:
        for r in rows[1:]:          # ignore the headers
            vals = transform_row(r)
            sql = concat_insert_sql(vals)
            try:
                cursor.execute(sql)
                conn.commit()
            except Exception as e:
                logger.error(e)
        # close db connection
        conn.close()

def concat_insert_sql(vals):
    sql_template = """
    INSERT INTO `sms_deliver_stats` (
      publish_date, message_id, phone_number, 
      message_type, deliver_status, price_usd, 
      part_number, total_parts, country
    ) 
    VALUES 
      (
        '%(publish_date)s', 
        '%(message_id)s', 
        '%(phone_number)s', 
        '%(message_type)s', 
        '%(deliver_status)s', 
        %(price_usd) s, 
        %(part_number) s, 
        %(total_parts) s, 
        '%(country)s'
      )
    """
    return sql_template % vals
        
def transform_row(line):
    """
    construct row dict
    """
    cols = line.split(',')
    row = {
        'publish_date': parse_timestamp(cols[0]),
        'message_id': cols[1],
        'phone_number': cols[2],
        'message_type':cols[3],
        'deliver_status': parse_status(cols[4]),
        'price_usd': cols[5],
        'part_number': cols[6],
        'total_parts': cols[7],
        'country': parse_phone(cols[2])
    }
    return row
    
def parse_timestamp(time_string_utc):
    """
    get date string from utc
    """
    return time_string_utc[:10]


def parse_status(status_text):
    if status_text.find('has been accepted by') > 0:
        return 'Success'
    else:
        return 'Fail'

def parse_phone(phone_number):
    """
    Get country name from e.164 phone_number
    """
    try:
        c = pycountry.countries.get(alpha_2=phone_country(phone_number))
        return c.name
    except Exception as e:
        return 'Unknown'
    
