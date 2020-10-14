"""
Scrapper set up as scrapping-function Lambda with scrape_ids_data_to_excel handler scrapes for a batch of yad2_ids all the relevant data into an Excel file.
The AWS scrapping-function Lambda is initialized by a new message containing yad2 ids arriving in an AWS SQS queue, which is then passed to the scrapper within the event var.
The Excel file is stored in AWS S3 bucket with name defined in DESTINATION_BUCKET_NAME env variable.
"""

import requests
import json
import datetime
import time
import os
import boto3
import pandas as pd
import io


current_datetime = datetime.datetime.now() # capture date/time at start of execution


def request_through_proxy(url):
    """
    Send get request to url though Proxy with max timeout of 20 sec, expects json response, retry until success, returns json object
    """
    proxies = {
        "http": "", # TODO
        "https": "" # TODO
    }
    response = requests.get(url=url, proxies=proxies, verify=False, timeout=10)
    try:
        data = json.loads(response.text)
        print(url + ": OK")
        return data
    except:
        print(url + ": RECAPTCHA")
        time.sleep(5)
        return request_through_proxy(url)


def get_mapping_booleans(structure):
    """
    unpack json nested dictionary into simple dictionary and return dictionary
    """
    index = 0
    mapping = {}
    try:
        while True:
            key = structure[index]['key']
            mapping[key] = index
            index += 1
    except:
        pass
    return mapping


def extract(json_response, expression):
    """
    Extract json paramter value contained in json_response safely (path to parameter contained in expression), return empty string if fail
    """
    try:
        return eval(expression)
    except:
        return ''


def scrape_apartments(yad2_ids):
    """
    Scrape relevant data for all yad2 ids contained in yad2_ids, return data as a Dataframe
    """
    column_names            = ['id', 'url', 'images', 'latitude', 'longitude', 'CityID', 'neighborhood', 'street', 'address_home_number', 'contact_name', 'type', 'phone_number', 'second_phone_number', 'date_added', 'date', 'HomeTypeID', 'price', 'fromRooms', 'main_title', 'info_text', 'air_conditioner', 'bars', 'elevator', 'kosher_kitchen', 'accessibility', 'renovated', 'shelter', 'warhouse', 'pandor_doors', 'furniture', 'balconies', 'square_meters', 'FromFloor', 'TotalFloor_text', 'parking', 'date_of_entry']
    df                      = pd.DataFrame(columns=column_names)
    rows                    = []
    for yad2_id in yad2_ids:
        try:
            rows.append(get_apartment_data(yad2_id))
        except:
            pass
    df = df.from_dict(rows, orient='columns')
    return df


def get_apartment_data(apartment_id):
    """
    Scrape for a yad2 id apartment_id all the relevant data into a dictionary, return dictionary
    """
    url                         = 'https://www.yad2.co.il/api/item/' + apartment_id
    url_contactinfo             = url + '/contactinfo'
    json_response               = request_through_proxy(url)
    json_response_contactinfo   = request_through_proxy(url_contactinfo)
    apartment_data = {'id': str(apartment_id), 'url': 'https://www.yad2.co.il/item/' + str(apartment_id)}
    apartment_data['images']                = extract(json_response, "json_response['images']")
    apartment_data['latitude']              = extract(json_response, "json_response['navigation_data']['coordinates']['latitude']")
    apartment_data['longitude']             = extract(json_response, "json_response['navigation_data']['coordinates']['longitude']")
    apartment_data['CityID']                = extract(json_response, "json_response['city_code']")
    apartment_data['neighborhood']          = extract(json_response, "json_response['neighborhood']")
    apartment_data['street']                = extract(json_response, "json_response['street']")
    apartment_data['address_home_number']   = extract(json_response, "json_response['address_home_number']")
    apartment_data['contact_name']          = extract(json_response, "json_response['contact_name']")
    apartment_data['phone_number']          = extract(json_response_contactinfo, "json_response['data']['phone_numbers'][0]['title']")
    apartment_data['second_phone_number']   = extract(json_response_contactinfo, "json_response['data']['phone_numbers'][1]['title']")
    apartment_data['type']                  = 'Properties' if "" == extract(json_response, "json_response['agency_contact_name']") else 'External'
    apartment_data['date_added']            = extract(json_response, "json_response['date_added']")
    apartment_data['date']                  = extract(json_response, "json_response['date']")
    apartment_data['HomeTypeID']            = extract(json_response, "json_response['media']['params']['HomeTypeID']")
    apartment_data['price']                 = extract(json_response, "json_response['price']")
    apartment_data['fromRooms']             = extract(json_response, "json_response['media']['params']['fromRooms']")
    apartment_data['main_title']            = extract(json_response, "json_response['main_title']")
    apartment_data['info_text']             = extract(json_response, "json_response['info_text']")
    mapping_booleans                        = get_mapping_booleans(json_response['additional_info_items_v2'])
    apartment_data['air_conditioner']       = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("air_conditioner", -1)) + "]['value']")
    apartment_data['bars']                  = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("bars", -1)) + "]['value']")
    apartment_data['elevator']              = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("elevator", -1)) + "]['value']")
    apartment_data['kosher_kitchen']        = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("kosher_kitchen", -1)) + "]['value']")
    apartment_data['accessibility']         = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("accessibility", -1)) + "]['value']")
    apartment_data['renovated']             = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("renovated", -1)) + "]['value']")
    apartment_data['shelter']               = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("shelter", -1)) + "]['value']")
    apartment_data['warhouse']              = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("warhouse", -1)) + "]['value']")
    apartment_data['pandor_doors']          = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("pandor_doors", -1)) + "]['value']")
    apartment_data['furniture']             = extract(json_response, "json_response['additional_info_items_v2'][" + str(mapping_booleans.get("furniture", -1)) + "]['value']")
    apartment_data['balconies']             = extract(json_response, "json_response['balconies']")
    apartment_data['square_meters']         = extract(json_response, "json_response['square_meters']")
    apartment_data['FromFloor']             = extract(json_response, "json_response['media']['params']['FromFloor']")
    apartment_data['TotalFloor_text']       = extract(json_response, "json_response['TotalFloor_text']")
    apartment_data['parking']               = extract(json_response, "json_response['parking']")
    apartment_data['date_of_entry']         = extract(json_response, "json_response['date_of_entry']")
    return apartment_data


def save_to_aws_s3(df, bucket_name):
    """
    Saves Dataframe df as an Excel file in AWS S3 with bucket name bucket_name, file name of format yad2_export_datetime=DATETIME.xlsx
    """
    client                  = boto3.client("s3", region_name="", aws_access_key_id="", aws_secret_access_key="") # TODO
    buffer                  = io.BytesIO()
    df.to_excel(buffer)
    body                    = buffer.getvalue()
    datetime_for_filename   = str(datetime.datetime.now()).replace(" ", "-").replace(":", "-").replace(".", "-")
    key                     = "yad2_export_datetime=" + datetime_for_filename + ".xlsx"
    client.put_object(Bucket=bucket_name, Body=body, Key=key, ACL="public-read")


def scrape_ids_data_to_excel(event, lambda_context):
    """
    AWS Lambda entry point, expects env variable DESTINATION_BUCKET_NAME to be set up and event var to pass a message containing yad2_ids from AWS SQS queue
    """
    try:

        print(str(event))
        yad2_ids    = eval(event["Records"][0]["body"])
        bucket_name = str(os.environ['DESTINATION_BUCKET_NAME'])
        df          = scrape_apartments(yad2_ids)
        save_to_aws_s3(df, bucket_name)

        return {
            "statusCode": 200, 
            "body": json.dumps("Scraping successful!")
        }

    except Exception as e:

        print("Error: " + str(e))
        return {
                "statusCode": 400, 
                "body": json.dumps(str(e))
        }
