"""
Scrape_ids set up as scrapping-broker-function Lambda with scrape_relevant_ids handler scrapes all yad2_ids of apartments updated in the last 24h
in the cities defined in YAD2_CITY_ID env variable like ["5000", "7900"] and sends the ids in batches to AWS SQS FIFO queue defined in SQS_PAGES_QUEUE_NAME
env variable for further processing.
"""

import requests
import json
import datetime
import time
import pymsteams
import os
import boto3


current_datetime = datetime.datetime.now() # capture date/time at start of execution


def request_through_proxy(url):
    """
    Send get request to url though Proxy with max timeout of 20 sec, expects json response, retry until success, returns json object
    """
    proxies = {
        "http": "", # TODO
        "https": "" # TODO
    }
    response = requests.get(url=url, proxies=proxies, verify=False, timeout=20)
    try:
        data = json.loads(response.text)
        print(url + ": OK")
        return data
    except:
        print(url + ": RECAPTCHA")
        time.sleep(5)
        return request_through_proxy(url)


def check_last_updated(date_raw):
    """
    Check date_raw in format Y-m-d H:M:S within last 24h according to the start time of execution (current_datetime global var), returns boolean
    """
    delta = current_datetime - datetime.datetime.strptime(date_raw, "%Y-%m-%d %H:%M:%S")
    return (delta.total_seconds() <= 24 * 60 * 60) # check listing updated in last 24h


def scrape_apartments(city_id, sqs_name):
    """
    Scrapes all yad2_id of apartments in city_id (like Tel Aviv = 5000) updated in the last 24h, enters ids in batches (a batch consists of all ids on one result page) into AWS SQS queue
    """
    apartment_ids           = []
    more_relevant_listings  = 3 # skip the first 3 listings updated after 24h -> some might be out of order and can result in premature termination of script
    current_page            = 1
    total_pages             = 1

    while current_page <= total_pages:
        if more_relevant_listings > 0:
            url = 'https://www.yad2.co.il/api/pre-load/getFeedIndex/realestate/forsale?city=' + str(city_id) + '&page=' + str(current_page)
            data = request_through_proxy(url)
            i = 0
            try:
                total_pages = data['feed']['total_pages']
            except:
                pass
            while len(data['feed']['feed_items']) > i: # get all listing yad2_id on a result page
                if more_relevant_listings > 0:
                    try:
                        apartment_ids.append(data['feed']['feed_items'][i]['id'])
                        if data['feed']['feed_items'][i]['ad_highlight_type'] == 'none':
                            if not check_last_updated(data['feed']['feed_items'][i]['date']):
                                more_relevant_listings = more_relevant_listings - 1
                    except:
                        pass
                i += 1
            send_ids_to_sqs(apartment_ids, sqs_name) # submit ids to sqs
            apartment_ids = []
        current_page += 1


def send_ids_to_sqs(apartment_ids, sqs_name):
    """
    Submits array apartment_ids containing all yad2_ids on a result page as a message to AWS SQS FIFO queue with name sqs_name
    """
    sqs = boto3.client("sqs", region_name="", aws_access_key_id="", aws_secret_access_key="") # TODO
    queue_url = "" + sqs_name # TODO
    message = str(apartment_ids)
    print(message)
    response = sqs.send_message(QueueUrl=queue_url, DelaySeconds=10, MessageBody=message) # Send message to SQS queue
    print(response)


def scrape_relevant_ids(event, lambda_context): 
"""
AWS Lambda entry point, expects env variable SQS_PAGES_QUEUE_NAME and YAD2_CITY_ID to be set up
"""

    try:

        print("Scraping Yad2 search result ids initialized.")
        sqs_name      = str(os.environ['SQS_PAGES_QUEUE_NAME'])
        yad2_city_ids = eval(str(os.environ['YAD2_CITY_ID']))
        for yad2_city_id in yad2_city_ids:
            print("Scraping Yad2 city: " + str(yad2_city_id))
            scrape_apartments(yad2_city_id, sqs_name)
        print("Scraping Yad2 search result ids done.")
        
        return {
            "statusCode": 200, 
            "body": json.dumps("Scraping successful!")
        }

    except Exception as e:

        return {
                "statusCode": 400, 
                "body": json.dumps(str(e))
        }
