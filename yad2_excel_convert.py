"""
yad2_excel_convert.py is set up as importPropertiesToSFF-function Lambda with yad2_import_excel_to_sf handler is triggered by a new Excel file in AWS S3 bucket.
The url of the file is passed within event var and the apartments data is uploaded to Salesforce (defined in env variables: SALESFORCE_DOMAIN, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN, SALESFORCE_USERNAME) 
and the HomeHero database (defined in HOMEHERO_ENV) and synced between both.
"""

import pandas as pd
import numpy as np
import datetime
import simple_salesforce  
import json
import re
import os
import pymsteams
import requests
from datetime import date


HOMEHERO_AUTHORIZATION_HEADERS = {
    "": "", # TODO
    "": "" # TODO
}
HOMEHERO_ENV = "-staging" # default


def teams_message(message):
    """
    Send message to teams channel as log
    """
    teams_channel = pymsteams.connectorcard("") # TODO
    teams_channel.text(message)
    teams_channel.send()
    return None


def prepare_data(path):
    """
    Load data from Excel file in path, perform validation and conversion to Salesforce format, return as dataframe
    """
    names = [
        "row",
        "yad2_id__c",
        "yad2_url__c",
        "images", 
        "latitude__c",
        "longitude__c",
        "Property_City__c", 
        "Neighborhoods_HH__c", 
        "pba__Address_pb__c", 
        "pba__Address_pb__c_num", 
        "contact_name", 
        "Mobile__c", 
        "Second_mobile__c",
        "type__c",
        "Created_Date__c",
        "last_update_date__c",
        "pba__PropertyType__c",
        "pba__ListingPrice_pb__c",
        "Room_HH__c",
        "title_in_websites__c",
        "pba__Description_pb__c",
        "Air_conditioner__c",
        "bars__c",
        "Elavator__c",
        "kosher_kitchen__c",
        "accessibility__c",
        "renovated__c",
        "shelter__c",
        "warhouse__c",
        "pandor_doors__c", 
        "furniture__c",
        "Balcony__c",
        "pba__SqFt_pb__c",
        "pba__Floor__c",
        "pba__NumberOfFloors__c",
        "Number_of_Parking_space__c",
        "entry_date__c"
        ]

    def REQUIRED_ID(x):
        if not str(x):
            print("hello")
            raise Exception("yad2 id missing")
        else:
            return str(x)

    def images(x):
        return str(x).replace("//", "https://")

    def latitude_longitude(x):
        try:
            y = float(x)
            return str(y)
        except:
            return None

    property_city_mapping = {
        168:   "Kfar Yona",
        5000:  "Tel Aviv Yafo",
        6300:  "Givatayim",
        6900:  "Kfar Saba",
        7400:  "Netanya",
        7900:  "Petah Tikva",
        8600:  "Ramat Gan",
        8700:  "Raanana"
    }

    def property_city(x):
        try:
            y = int(x)
            return property_city_mapping.get(y) # default None
        except:
            return None

    def neighborhood(x):
        return str(x).replace('שיכון מפ"ם', 'שיכון מפם')

    def address(x):
        return str(x).replace("None", "")

    def REQUIRED_MOBILE(x):
        if not x:
            raise Exception("mobile missing")
        else:
            return str(x).replace("-", "")

    def type_c(x):
        if str(x) not in ["Properties", "External"]:
            raise Exception("unknown listing type" + str(x)) 
        else:
            return str(x)

    def created_date(x):
        try:
            y = str(x[0:4] + "-" + x[5:7] + "-" + x[8:10] + "T" + x[11:19] + ".000+0300")
            if re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\.000\+0300").match(y):
                return y
            else:
                return None
        except:
            return None

    def last_update(x):
        try:
            if x == "עודכן היום":
                y = str(datetime.datetime.now())[0:10]
            else:
                z = re.search("(\d+/\d+/\d+)", x).group(1)
                y = str(z[6:10] + "-" + z[3:5] + "-" + z[0:2])
            if re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]").match(y):
                return y
            else:
                None
        except:
            return None

    property_type_mapping = {
        1:  "Apartment",
        3:  "Garden Apartment",
        5:  "Two Family Cottage",
        6:  "Penthouse/Rooftop Apartment",
        33: "Plot",
        7:  "Duplex",
        39: "Two Family Cottage",
        25: "Holiday unit",
        49: "Basement",
        51: "Triplex",
        11: "Residential Unit",
        32: "Other",
        55: "Other",
        61: "Other",
        44: "Other",
        4:  "Studio / loft",
        45: "Other",
        30: "Other",
        50: "Other",
        41: "Other"
    }

    def property_type(x):
        try:
            y = int(x)
            return property_type_mapping.get(y) # default None
        except:
            return None

    def listing_price(x):
        if not x or x == "לא צוין מחיר":
            return None
        try: 
            y = str(x).replace(",", "").replace("₪", "").replace(" ", "")
            z = float(y)
            return str(z)
        except:
            return None

    def room(x):
        try:
            y = float(x)
            return str(y)
        except:
            return None

    def title_in_websites(x):
        if not x:
            return None
        return str(x).replace("&nbsp;", " ")[:250] # cut after 250 chars

    def description(x):
        if not x:
            return None
        else:
            return str(x)[:31950]

    def bool_to_string(x):
        y = str(x).lower().strip()
        if y == "true" or y == "false":
            return str(y)
        else:
            return "false"

    def balcony(x):
        try:
            y = int(x)
            if y in [0, 1, 2, 3]:
                return str(y)
            elif y > 3:
                return "3"
            else:
                return "0"
        except:
            return None

    def sqft(x):
        try:
            y = int(x)
            return str(y)
        except:
            return None

    def elevator(x):
        y = bool_to_string(x)
        if not y:
            return None
        else:
            return y.replace("true", "1").replace("false", "0")

    def floor(x):
        try:
            y = int(x)
            return str(y)
        except:
            return None

    def number_floor(x):
        try:
            y = int(x)
            return str(y)
        except:
            return None

    def parking_space(x):
        try:
            y = int(x)
            if y in [0, 1, 2, 3, 4, 5]:
                return str(y)
            elif y > 5:
                return "5"
            else:
                return "0"
        except:
            return None
    
    def entry_date(x):
        try:
            y = str(x[6:10] + "-" + x[3:5] + "-" + x[0:2])
            if re.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]").match(y):
                return y
            else:
                None
        except: 
            return None

    # Data Mappings -> validate before passing to salesforce
    converters = {
        "yad2_id__c":                       REQUIRED_ID,
        "yad2_url__c":                      REQUIRED_ID,
        "images":                           images,
        "latitude__c":                      latitude_longitude,
        "longitude__c":                     latitude_longitude,
        "Property_City__c":                 property_city,
        "Neighborhoods_HH__c":              neighborhood,
        "pba__Address_pb__c":               address,
        "pba__Address_pb__c_num":           address,
        "contact_name":                     str,
        "Mobile__c":                        REQUIRED_MOBILE,
        "Second_mobile__c":                 address,
        "type__c":                          type_c,
        "Created_Date__c":                  created_date, 
        "last_update_date__c":              last_update, 
        "pba__PropertyType__c":             property_type,
        "pba__ListingPrice_pb__c":          listing_price,
        "Room_HH__c":                       room,
        "title_in_websites__c":             title_in_websites,
        "pba__Description_pb__c":           description,
        "Air_conditioner__c":               bool_to_string,
        "bars__c":                          bool_to_string,
        "Elavator__c":                      elevator,
        "kosher_kitchen__c":                bool_to_string,
        "accessibility__c":                 bool_to_string,
        "renovated__c":                     bool_to_string,
        "shelter__c":                       bool_to_string,
        "warhouse__c":                      bool_to_string,
        "pandor_doors__c":                  bool_to_string,
        "furniture__c":                     bool_to_string,
        "Balcony__c":                       balcony,
        "pba__SqFt_pb__c":                  sqft,
        "pba__Floor__c":                    floor,
        "pba__NumberOfFloors__c":           number_floor,
        "Number_of_Parking_space__c":       parking_space,
        "entry_date__c":                    entry_date
    }

    # read file into a DataFrame
    try:
        df = pd.read_excel(io=path, names=names, converters=converters, na_filter=False)
    except Exception as e:
        raise Exception("Failed to parse Excel: " + str(e))
    
    #contact_name split into first and last name
    try:
        df["lastNameForFormula__c"] = ""
        df["First_Name_HH__c"]      = ""
        for row_index in df.contact_name.index:
            split_name = df.at[row_index, "contact_name"].rsplit(" ", maxsplit=1)
            if len(split_name) == 2:
                df.at[row_index, "lastNameForFormula__c"] = split_name[1][0:30]
                df.at[row_index, "First_Name_HH__c"] = split_name[0][0:30]
            elif len(split_name) == 1:
                df.at[row_index, "First_Name_HH__c"] = ""
                df.at[row_index, "lastNameForFormula__c"] = "unknown" if not (split_name[0][0:30]) else split_name[0][0:30]
        df = df.drop("contact_name", 1)
    except:
        raise Exception("Failed to create last & first name")

    #merge street name & house number to address
    df["pba__Address_pb__c"] = df["pba__Address_pb__c"] + " " + df["pba__Address_pb__c_num"]
    df = df.drop("pba__Address_pb__c_num", 1)

    #validate floor & max floor
    for row_index in df.pba__SqFt_pb__c.index:
        if df.at[row_index, "pba__SqFt_pb__c"] == df.at[row_index, "pba__Floor__c"]:
            df.at[row_index, "pba__Floor__c"]          = ""
            df.at[row_index, "pba__NumberOfFloors__c"] = ""

    #additional parameters
    df["Source__c"]                         = "Yad2"
    df["pba__Status__c"]                    = "New"
    df["BO_Status__c"]                      = "Pending"
    df["Number_of_missed_calls__c"]         = "0"
    df["Seller_side_commission_signed__c"]  = "0"
    df["OwnerId"]                           = "0054J000002EgHFQA0"
    df["Campaign_name__c"]                  = ""    
    
    #type of property
    df["Owning_work_on_the_property__c"]    = "Private"
    df["pba__ContactType_pb__c"]            = "Seller"
    df["Exclusivity_other_agent__c"]        = "false"
    for row in df.type__c.index:
        if df.at[row, "type__c"] == "External":
            df.at[row, "Owning_work_on_the_property__c"]    = "Collaboration"
            df.at[row, "pba__ContactType_pb__c"]            = "Real Estate Agent"
            df.at[row, "Exclusivity_other_agent__c"]        = "true"
   
    #for images
    df["pba__Category__c"]                  = "Images"
    df["pba__IsExternalLink__c"]            = "true"
    df["pba__IsOnExpose__c"]                = "true"

    #for sync
    df["contact_salesforce_id"] = ""
    df["contact_backoffice_id"] = ""
    df["listing_salesforce_id"] = ""
    df["listing_backoffice_id"] = ""
    df["checkIfSync__c"]        = "true"

    return df


def open_salesforce_connection(salesforce_domain, salesforce_password, salesforce_security_token, salesforce_username):
    """
    open connection with Salesforce account, note domain=test for Sandbox or domain=login for prod
    """
    session_id, instance = simple_salesforce.SalesforceLogin(username = salesforce_username, password = salesforce_password, security_token = salesforce_security_token, domain = salesforce_domain)
    sf = simple_salesforce.Salesforce(instance = instance, session_id = session_id)
    return sf


def send_listings(sf, df):
    """
    send listings contained in df through Salesforce connection sf, set listing and seller contact and images
    """
    # for error handling
    last_contact = ""
    last_listing = ""
    
    for row in df.yad2_id__c.index:
        
        print(row)
        try:
            
            # create / update contact salesforce
            print("contact salesforce") ####
            df.at[row, "contact_salesforce_id"]                                         = salesforce_contact(sf, df, row)
            last_contact                                                                = df.at[row, "contact_salesforce_id"]

            # create / update listing salesforce
            print("listing salesforce") ####
            df.at[row, "listing_salesforce_id"]                                         = salesforce_listing(sf, df, row)
            last_listing                                                                = df.at[row, "listing_salesforce_id"]

            # create images (property media)
            print("images salesforce") ####
            salesforce_images(sf, df, row)

            # sync contact & listing to Homehero database
            print("contact & listing backoffice") ####
            (df.at[row, "listing_backoffice_id"], df.at[row, "contact_backoffice_id"])  = sync_to_backoffice(sf, df, row)

            # link listing in contact, update bo_id of contact
            print("update contact salesforce") ####
            update_salesforce_contact(sf, df, row)

            # update bo_id of listing
            print("update listing salesforce") ####
            update_salesforce_listing(sf, df, row)
        
        except:
            raise Exception("Uploading to Salesforce failed, last sucessful contact uploaded: " + last_contact + ", last sucessful listing uploaded: " + last_listing)


def salesforce_contact(sf, df, row, update=False):
    """
    update Salesforce seller contact according to phone number, else create new Salesforce seller contact
    """
    try:       
        # get existing contact (Mobile)
        contact = sf.query("SELECT id FROM Contact WHERE MobilePhone = '" + df.at[row, "Mobile__c"].replace("-", "") + "' LIMIT 1")
        contact_id = contact["records"][0]["Id"]
        sf.Contact.update(contact_id, {
            "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
        })
        print("Contact found & assigned: " + contact_id) ###
        teams_message("Contact found & assigned: " + contact_id) ###
            
    except:
        # create new contact
        contact = sf.Contact.create({
            "First_Name_HH__c":                 df.at[row, "First_Name_HH__c"],
            "lastNameForFormula__c":            df.at[row, "lastNameForFormula__c"],
            "LastName":                         df.at[row, "lastNameForFormula__c"],
            "pba__ContactType_pb__c":           df.at[row, "pba__ContactType_pb__c"],
            "MobilePhone":                      df.at[row, "Mobile__c"],
            "Second_mobile__c":                 df.at[row, "Second_mobile__c"],
            "OwnerId":                          df.at[row, "OwnerId"]
        })
        contact_id = contact["id"] ###
        print("Contact created: " + contact_id) ###
        sf.Contact.update(contact_id, {
            "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
        })
        teams_message("Contact created: " + contact_id) ###

    return contact_id


def update_salesforce_contact(sf, df, row):
    """
    update Salesforce seller contact with link to listing
    """
    sf.Contact.update(df.at[row, "contact_salesforce_id"], {
        "backoffice_id__c":                 df.at[row, "contact_backoffice_id"],
        "Assigned_Property__c":             df.at[row, "listing_salesforce_id"]
    })
    sf.Contact.update(df.at[row, "contact_salesforce_id"], {
        "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
    })
    return None


def salesforce_listing(sf, df, row):
    """
    update Salesforce listing according to yad2 id, else update Salesforce listing according to phone number and address and floor, else create new Salesforce listing
    """
    try:
                
        # get existing listing (yad2 id)
        listing = sf.query("SELECT id, pba__ListingPrice_pb__c FROM pba__Listing__c WHERE yad2_id__c = '" + df.at[row, 'yad2_id__c'] + "' LIMIT 1")
                
        try:
                    
            listing_id = listing["records"][0]["Id"]
                
        except:
                    
            # get existing listing (address & floor & mobile)
            listing = sf.query("SELECT id, pba__ListingPrice_pb__c FROM pba__Listing__c WHERE pba__Address_pb__c = '" + df.at[row, 'pba__Address_pb__c'] + "' AND Mobile__c = '" + df.at[row, 'Mobile__c'] + "' AND pba__Floor__c = '" + df.at[row, 'pba__Floor__c'] + "' LIMIT 1")
            listing_id = listing["records"][0]["Id"]
                
        # update listing
        sf.pba__Listing__c.update(listing_id, {
            "yad2_id__c":                       df.at[row, "yad2_id__c"],
            "yad2_url__c":                      df.at[row, "yad2_url__c"],
            "latitude__c":                      df.at[row, "latitude__c"],
            "longitude__c":                     df.at[row, "longitude__c"],
            "Property_City__c":                 df.at[row, "Property_City__c"], 
            "Neighbrhoods_Yad2__c":             df.at[row, "Neighborhoods_HH__c"],
            "pba__Address_pb__c":               df.at[row, "pba__Address_pb__c"], 
            "Mobile__c":                        df.at[row, "Mobile__c"],
            "Second_mobile__c":                 df.at[row, "Second_mobile__c"],
            "Created_Date__c":                  df.at[row, "Created_Date__c"],
            "last_update_date__c":              df.at[row, "last_update_date__c"],
            "pba__PropertyType__c":             df.at[row, "pba__PropertyType__c"],
            "pba__ListingPrice_pb__c":          df.at[row, "pba__ListingPrice_pb__c"],
            "Room_HH__c":                       df.at[row, "Room_HH__c"],
            "title_in_websites__c":             df.at[row, "title_in_websites__c"],
            "pba__Description_pb__c":           df.at[row, "pba__Description_pb__c"],
            "Air_conditioner__c":               df.at[row, "Air_conditioner__c"],
            "bars__c":                          df.at[row, "bars__c"],
            "Elavator__c":                      df.at[row, "Elavator__c"],
            "kosher_kitchen__c":                df.at[row, "kosher_kitchen__c"],
            "accessibility__c":                 df.at[row, "accessibility__c"],
            "renovated__c":                     df.at[row, "renovated__c"],
            "shelter__c":                       df.at[row, "shelter__c"],
            "warhouse__c":                      df.at[row, "warhouse__c"],
            "pandor_doors__c":                  df.at[row, "pandor_doors__c"],
            "furniture__c":                     df.at[row, "furniture__c"],
            "Balcony__c":                       str(df.at[row, "Balcony__c"]),
            "pba__SqFt_pb__c":                  df.at[row, "pba__SqFt_pb__c"],
            "pba__Floor__c":                    df.at[row, "pba__Floor__c"],
            "pba__NumberOfFloors__c":           df.at[row, "pba__NumberOfFloors__c"],
            "Number_of_Parking_space__c":       df.at[row, "Number_of_Parking_space__c"],
            "entry_date__c":                    df.at[row, "entry_date__c"],
            "Source__c":                        df.at[row, "Source__c"],
            "Owning_work_on_the_property__c":   df.at[row, "Owning_work_on_the_property__c"],
            "Number_of_missed_calls__c":        df.at[row, "Number_of_missed_calls__c"],
            "Seller_side_commission_signed__c": df.at[row, "Seller_side_commission_signed__c"],
            "OwnerId":                          df.at[row, "OwnerId"],
            "Exclusivity_other_agent__c":       df.at[row, "Exclusivity_other_agent__c"],
            "Campaign_name__c":                 df.at[row, "Campaign_name__c"],
            "pba__PropertyOwnerContact_pb__c":  df.at[row, "contact_salesforce_id"]
        })
        sf.pba__Listing__c.update(listing_id, {
            "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
        })
        print("Listing updated: " + listing_id) ###
        teams_message("Listing updated: " + listing_id) ###
                
    except:
            
        # create listing
        listing = sf.pba__Listing__c.create({
            "yad2_id__c":                       df.at[row, "yad2_id__c"],
            "yad2_url__c":                      df.at[row, "yad2_url__c"],
            "latitude__c":                      df.at[row, "latitude__c"],
            "longitude__c":                     df.at[row, "longitude__c"],
            "Property_City__c":                 df.at[row, "Property_City__c"], 
            "Neighbrhoods_Yad2__c":             df.at[row, "Neighborhoods_HH__c"], 
            "pba__Address_pb__c":               df.at[row, "pba__Address_pb__c"], 
            "Mobile__c":                        df.at[row, "Mobile__c"],
            "Second_mobile__c":                 df.at[row, "Second_mobile__c"],
            "type__c":                          df.at[row, "type__c"],
            "Created_Date__c":                  df.at[row, "Created_Date__c"],
            "last_update_date__c":              df.at[row, "last_update_date__c"],
            "pba__PropertyType__c":             df.at[row, "pba__PropertyType__c"],
            "pba__ListingPrice_pb__c":          df.at[row, "pba__ListingPrice_pb__c"],
            "Room_HH__c":                       df.at[row, "Room_HH__c"],
            "title_in_websites__c":             df.at[row, "title_in_websites__c"],
            "pba__Description_pb__c":           df.at[row, "pba__Description_pb__c"],
            "Air_conditioner__c":               df.at[row, "Air_conditioner__c"],
            "bars__c":                          df.at[row, "bars__c"],
            "Elavator__c":                      df.at[row, "Elavator__c"],
            "kosher_kitchen__c":                df.at[row, "kosher_kitchen__c"],
            "accessibility__c":                 df.at[row, "accessibility__c"],
            "renovated__c":                     df.at[row, "renovated__c"],
            "shelter__c":                       df.at[row, "shelter__c"],
            "warhouse__c":                      df.at[row, "warhouse__c"],
            "pandor_doors__c":                  df.at[row, "pandor_doors__c"],
            "furniture__c":                     df.at[row, "furniture__c"],
            "Balcony__c":                       str(df.at[row, "Balcony__c"]),
            "pba__SqFt_pb__c":                  df.at[row, "pba__SqFt_pb__c"],
            "pba__Floor__c":                    df.at[row, "pba__Floor__c"],
            "pba__NumberOfFloors__c":           df.at[row, "pba__NumberOfFloors__c"],
            "Number_of_Parking_space__c":       df.at[row, "Number_of_Parking_space__c"],
            "entry_date__c":                    df.at[row, "entry_date__c"],
            "Source__c":                        df.at[row, "Source__c"],
            "pba__Status__c":                   df.at[row, "pba__Status__c"],
            "BO_Status__c":                     df.at[row, "BO_Status__c"],
            "Owning_work_on_the_property__c":   df.at[row, "Owning_work_on_the_property__c"],
            "Number_of_missed_calls__c":        df.at[row, "Number_of_missed_calls__c"],
            "Seller_side_commission_signed__c": df.at[row, "Seller_side_commission_signed__c"],
            "OwnerId":                          df.at[row, "OwnerId"],
            "Exclusivity_other_agent__c":       df.at[row, "Exclusivity_other_agent__c"],
            "Campaign_name__c":                 df.at[row, "Campaign_name__c"],
            "pba__PropertyOwnerContact_pb__c":  df.at[row, "contact_salesforce_id"]
        })
        listing_id = listing["id"]
        sf.pba__Listing__c.update(listing_id, {
            "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
        })
        print("Listing created: " + listing_id) ###
        teams_message("Listing created: " + listing_id) ###

    return listing_id


def update_salesforce_listing(sf, df, row):
    """
    update Salesforce listing with BO id
    """
    sf.pba__Listing__c.update(df.at[row, "listing_salesforce_id"], {
        "backoffice_ID__c":                 df.at[row, "listing_backoffice_id"]
    })
    sf.pba__Listing__c.update(df.at[row, "listing_salesforce_id"], {
        "checkIfSync__c":                   df.at[row, "checkIfSync__c"]
    })
    return None


def salesforce_images(sf, df, row):
    """
    fetch and delete all images currently related to listing, set default image, create and link new yad2 images
    """
    listing_property = sf.query("SELECT pba__Property__c FROM pba__Listing__c WHERE ID = '" + df.at[row, "listing_salesforce_id"] + "' LIMIT 1")
    listing_property_id = listing_property["records"][0]["pba__Property__c"]
        
    images = sf.query("SELECT id FROM pba__PropertyMedia__c WHERE pba__Property__c = '" + listing_property_id + "' LIMIT 100")
    images_id = list(map(lambda x: x["Id"], images["records"]))
            
    print("Deleting images: " + str(images_id)) ###
        
    # delete images
    for image_id in images_id: 
        sf.pba__PropertyMedia__c.delete(image_id)
        
    images = str(df.at[row, "images"]).replace("[", "").replace("]", "").replace("'", "").split(',')

    # set default first images HOM-1254
    if df.at[row, "Owning_work_on_the_property__c"] == "Collaboration":
        images.insert(0, "") # TODO
    elif df.at[row, "Owning_work_on_the_property__c"] == "Private":
        images.insert(0, "") # TODO

    # upload images
    for image in images:
                
        sf.pba__PropertyMedia__c.create({ 
            "pba__Category__c":             df.at[row, "pba__Category__c"],
            "pba__Title__c":                image.replace("https://img.yad2.co.il/Pic/", ""),
            "pba__Property__c":             listing_property_id,
            "pba__IsExternalLink__c":       df.at[row, "pba__IsExternalLink__c"],
            "pba__ExternalLink__c":         image,
            "pba__IsOnExpose__c":           df.at[row, "pba__IsOnExpose__c"]
        })
            
    print("Created images: " + str(images)) ###


def sync_to_backoffice(sf, df, row):
    """
    syncronize a listing and seller contact combination to the database and exchange backoffice and salesforce ids
    """
    salesforce_contact = sf.Contact.get(df.at[row, "contact_salesforce_id"])
    salesforce_listing = sf.pba__Listing__c.get(df.at[row, "listing_salesforce_id"])
    print("userBackOfficeID__c: " + str(salesforce_listing["userBackOfficeID__c"]))
    users_found = find_user(salesforce_contact)

    if users_found == []:

        salesforce_contact["backoffice_id__c"] = create_user(salesforce_contact)
        
        try:
            
            (listing_backoffice_id, contact_backoffice_id) = merge_apartment(salesforce_listing, salesforce_contact)
        
        except:

            apartments_found = find_apartment(salesforce_listing)
            if apartments_found != []: salesforce_listing["backoffice_ID__c"] = str(apartments_found[0]["id"])
            (listing_backoffice_id, contact_backoffice_id) = update_apartment(salesforce_listing, salesforce_contact)
        
        return (listing_backoffice_id, contact_backoffice_id)

    else:
        
        backoffice_index = ["salesforce_id", "id", "email"]
        salesforce_index = ["Id", "backoffice_id__c", "Email"]
        
        for index in range(3):
            
            if index == 2 and not salesforce_contact["Email"]:
                salesforce_contact["Email"] = "phone" + salesforce_contact["MobilePhone"] + "" # TODO

            for user_found in users_found:
                
                if user_found[backoffice_index[index]] == salesforce_contact[salesforce_index[index]]:
                    
                    salesforce_contact["backoffice_id__c"] = user_found["id"]
                    update_user(salesforce_contact)

                    try:
            
                        (listing_backoffice_id, contact_backoffice_id) = merge_apartment(salesforce_listing, salesforce_contact)
        
                    except:

                        apartments_found = find_apartment(salesforce_listing)
                        if apartments_found != []: salesforce_listing["backoffice_ID__c"] = str(apartments_found[0]["id"])
                        (listing_backoffice_id, contact_backoffice_id) = update_apartment(salesforce_listing, salesforce_contact)

                    return (listing_backoffice_id, contact_backoffice_id)


def find_user(salesforce_contact):
    """
    find any user (contact) in the database with same sf id or bo id or email
    """
    find_query = {
        "select":["id","email","salesforce_id"],
        "where":{
            "type":"and",
            "conditions":[{
                "type":"isNull", 
                "field":"roleUserRelations.role_id"
            },{
                "type":"or", 
                "conditions":[{
                    "type":"equal", 
                    "field":"salesforce_id",
                    "value":salesforce_contact["Id"]
                }]
            }]
        },
        "relations":[{
            "relation":"roleUserRelations",
            "condition":{
                "type":"equal",
                "field":"roleUserRelations.role_id",
                "negate":"true",
                "value":2
            }
        }]
    }

    if salesforce_contact["backoffice_id__c"]:
        find_query["where"]["conditions"][1]["conditions"].append({
            "type": "equal",
            "field":"id",
            "value":str(int(salesforce_contact["backoffice_id__c"]))
        })
                                
    if salesforce_contact["Email"] or salesforce_contact["MobilePhone"]:                      
        find_query["where"]["conditions"][1]["conditions"].append({
            "type": "equal",
            "field":"email",
            "value":"phone" + salesforce_contact["MobilePhone"] + "" if not salesforce_contact["Email"] else salesforce_contact["Email"] # TODO
        })

    print(find_query) ####
    response = requests.post("" + HOMEHERO_ENV + "", headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(find_query)) # TODO
    print(response.text) ####
    return response.json()["data"]["items"]


def update_user(salesforce_contact):
    """
    update a user (seller) in the database
    """
    bool_to_int = {True: 1, False: 0}

    def check_substring(sub_word, word):
        try:
            return sub_word in word
        except: # word = None
            return False

    updated_user = {
        "buyer":            bool_to_int.get(check_substring("Buyer", salesforce_contact["pba__ContactType_pb__c"])),
        "seller":           bool_to_int.get(check_substring("Seller", salesforce_contact["pba__ContactType_pb__c"])),
        "salesforce_id":    salesforce_contact["Id"]
    }
    if salesforce_contact["Email"]:                 updated_user["email"] =         salesforce_contact["Email"]
    if salesforce_contact["First_Name_HH__c"]:      updated_user["first_name"] =    salesforce_contact["First_Name_HH__c"]
    if salesforce_contact["lastNameForFormula__c"]: updated_user["last_name"] =     salesforce_contact["lastNameForFormula__c"]
    if salesforce_contact["MobilePhone"]:           updated_user["phone"] =         salesforce_contact["MobilePhone"]
    if salesforce_contact["Second_mobile__c"]:      updated_user["phone_2"] =       salesforce_contact["Second_mobile__c"]

    print(updated_user) ####
    response = requests.put("" + HOMEHERO_ENV + "" + str(int(salesforce_contact["backoffice_id__c"])), headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(updated_user)) # TODO
    print(response.text) ####
    return None


def create_user(salesforce_contact):
    """
    create a new user (seller) in the database
    """ 
    bool_to_int = {True: 1, False: 0}

    def check_substring(sub_word, word):
        try:
            return sub_word in word
        except: # word = None
            return False

    created_user = {
        "admin_id":         0,
        "buyer":            bool_to_int.get(check_substring("Buyer", salesforce_contact["pba__ContactType_pb__c"])),
        "email":            "phone" + salesforce_contact["MobilePhone"] + "" if not salesforce_contact["Email"] else salesforce_contact["Email"], # TODO
        "email_token":      "",
        "first_login":      0,
        "first_name":       salesforce_contact["First_Name_HH__c"],
        "last_name":        salesforce_contact["lastNameForFormula__c"],
        "password":         "",
        "phone":            salesforce_contact["MobilePhone"],
        "phone_2":          salesforce_contact["Second_mobile__c"],
        "salesforce_id":    salesforce_contact["Id"],
        "seller":           bool_to_int.get(check_substring("Seller", salesforce_contact["pba__ContactType_pb__c"])),
        "status":           1,
        "type":             0
    }

    print(created_user) ####
    response = requests.post("" + HOMEHERO_ENV + "", headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(created_user)) # TODO
    print(response.text) ####
    return response.headers["location"].replace("/", "")


def merge_apartment(salesforce_listing, salesforce_contact):
    """
    create or update existing apartment in the database, might return exception if mismatch between sf id and bo id
    """
    city_id = {
        "Petah Tikva":      7900,
        "Tel Aviv Yafo":    5000,
        "Givatayim":        6300,
        "Ramat Gan":        8600,
        "Kfar Yona":        168,
        "Kfar Saba":        6900,
        "Netanya":          7400,
        "Raanana":          8700
    }

    status = {
        "Inactive":         0,
        "Published":        1,
        "New":              2,
        "Sold":             4,
        "Dropped":          5,
        "Pending":          6
    }

    item_type = {
        "External":         2,
        "Project":          1,
        "Properties":       0
    }

    bool_to_int = {
        True:               1,
        False:              0
    }

    def exclusivity_days(end_date, start_date):
        try:
            return (date.fromisoformat(str(end_date))-date.fromisoformat(str(start_date))).days
        except:
            return None

    backoffice_listing = {
        "id":                           salesforce_listing["backoffice_ID__c"],
        "address":                      salesforce_listing["pba__AddressText_pb__c"],
        "admin_id":                     salesforce_listing["userBackOfficeID__c"],
        "agent_id":                     salesforce_listing["userBackOfficeID__c"],
        "apartment_number":             salesforce_listing["apartmentNumber__c"],
        "status":                       status.get(salesforce_listing["BO_Status__c"]),
        "balconies":                    salesforce_listing["Balcony__c"],
        "balconies_size":               salesforce_listing["Balcony_size__c"],
        "city_id":                      city_id.get(salesforce_listing["Property_City__c"]),
        "construction_year":            salesforce_listing["pba__YearBuilt_pb__c"],
        "contract_exclusivity_date":    salesforce_listing["exclusivity_start_date__c"],
        "contract_exclusivity_days":    exclusivity_days(salesforce_listing["Exclusivity_end_date_Date__c"], salesforce_listing["exclusivity_start_date__c"]),
        "exclusivity":                  bool_to_int.get(salesforce_listing["Exclusivity__c"]),
        "floor":                        salesforce_listing["pba__Floor__c"],
        "house_number":                 re.sub(r'[^0123456789]+', '', salesforce_listing["pba__AddressText_pb__c"]).strip() if salesforce_listing["pba__AddressText_pb__c"] else None,
        "item_type":                    item_type.get(salesforce_listing["type__c"]),
        "languages":                    [{"keywords": "", "project_badge": "", "short_content": "", "project_badge_list": "", "project_badge_list_2": ""}],
        "max_floors":                   salesforce_listing["pba__NumberOfFloors__c"],
        "meeting_address":              salesforce_listing["pba__AddressText_pb__c"],
        "neighborhood":                 {"name": salesforce_listing["Neighborhoods_HH__c"]},
        "no_of_elevators":              salesforce_listing["Elavator__c"],
        "parking_number":               salesforce_listing["Number_of_Parking_space__c"],
        "price":                        salesforce_listing["pba__ListingPrice_pb__c"],
        "publish_price":                0,
        "rooms":                        salesforce_listing["Room_HH__c"],
        "salesforce_id":                salesforce_listing["Id"],
        "size":                         salesforce_listing["pba__SqFt_pb__c"],
        "sold_price":                   0,
        "sq_meter_price":               salesforce_listing["Price_sq__c"],
        "storage_size":                 salesforce_listing["Storage_Area__c"],
        "street":                       re.sub(r'[0123456789]+', '', salesforce_listing["pba__AddressText_pb__c"]).strip() if salesforce_listing["pba__AddressText_pb__c"] else None,
        "type":                         {"name": salesforce_listing["pba__PropertyType__c"], "language": "en-2"},
        "user_id":                      str(int(salesforce_contact["backoffice_id__c"])),
        "video":                        None
    }
    
    print(backoffice_listing) ####
    response = requests.post("" + HOMEHERO_ENV + "", headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(backoffice_listing)) # TODO
    print(response.text) ####
    
    listing_backoffice_id = response.json()["data"]["item"]["id"]
    contact_backoffice_id = response.json()["data"]["item"]["user_id"]
    return (listing_backoffice_id, contact_backoffice_id)


def find_apartment(salesforce_listing):
    """
    find apartment in database with sf id and fetch bo id
    """
    find_query = {
        "select":["id"],
        "where":{
            "type":"equal",
            "field":"salesforce_id",
            "value":salesforce_listing["Id"]
        }
    }

    print(find_query) ####
    response = requests.post("" + HOMEHERO_ENV + "", headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(find_query)) # TODO
    print(response.text) ####
    return response.json()["data"]["items"]


def update_apartment(salesforce_listing, salesforce_contact):
    """
    update apartment in the database with given bo id
    """
    city_id = {
        "Petah Tikva":      7900,
        "Tel Aviv Yafo":    5000,
        "Givatayim":        6300,
        "Ramat Gan":        8600,
        "Kfar Yona":        168,
        "Kfar Saba":        6900,
        "Netanya":          7400,
        "Raanana":          8700
    }

    status = {
        "Inactive":         0,
        "Published":        1,
        "New":              2,
        "Sold":             4,
        "Dropped":          5,
        "Pending":          6
    }

    item_type = {
        "External":         2,
        "Project":          1,
        "Properties":       0
    }

    bool_to_int = {
        True:               1,
        False:              0
    }

    def exclusivity_days(end_date, start_date):
        try:
            return (date.fromisoformat(str(end_date))-date.fromisoformat(str(start_date))).days
        except:
            return None

    backoffice_listing = {
        "address":                      salesforce_listing["pba__AddressText_pb__c"],
        "agent_id":                     salesforce_listing["userBackOfficeID__c"],
        "apartment_number":             salesforce_listing["apartmentNumber__c"],
        "status":                       status.get(salesforce_listing["BO_Status__c"]),
        "balconies":                    salesforce_listing["Balcony__c"],
        "balconies_size":               salesforce_listing["Balcony_size__c"],
        "city_id":                      city_id.get(salesforce_listing["Property_City__c"]),
        "construction_year":            salesforce_listing["pba__YearBuilt_pb__c"],
        "contract_exclusivity_date":    salesforce_listing["exclusivity_start_date__c"],
        "contract_exclusivity_days":    exclusivity_days(salesforce_listing["Exclusivity_end_date_Date__c"], salesforce_listing["exclusivity_start_date__c"]),
        "exclusivity":                  bool_to_int.get(salesforce_listing["Exclusivity__c"]),
        "floor":                        salesforce_listing["pba__Floor__c"],
        "house_number":                 re.sub(r'[^0123456789]+', '', salesforce_listing["pba__AddressText_pb__c"]).strip() if salesforce_listing["pba__AddressText_pb__c"] else None,
        "item_type":                    item_type.get(salesforce_listing["type__c"]),
        "max_floors":                   salesforce_listing["pba__NumberOfFloors__c"],
        "meeting_address":              salesforce_listing["pba__AddressText_pb__c"],
        "neighborhood":                 {"name": salesforce_listing["Neighborhoods_HH__c"]},
        "no_of_elevators":              salesforce_listing["Elavator__c"],
        "parking_number":               salesforce_listing["Number_of_Parking_space__c"],
        "price":                        salesforce_listing["pba__ListingPrice_pb__c"],
        "rooms":                        salesforce_listing["Room_HH__c"],
        "salesforce_id":                salesforce_listing["Id"],
        "size":                         salesforce_listing["pba__SqFt_pb__c"],
        "sq_meter_price":               salesforce_listing["Price_sq__c"],
        "storage_size":                 salesforce_listing["Storage_Area__c"],
        "street":                       re.sub(r'[0123456789]+', '', salesforce_listing["pba__AddressText_pb__c"]).strip() if salesforce_listing["pba__AddressText_pb__c"] else None,
        "type":                         {"name": salesforce_listing["pba__PropertyType__c"], "language": "en-2"},
        "user_id":                      str(int(salesforce_contact["backoffice_id__c"]))
    }

    print(backoffice_listing) ####
    response = requests.put("" + HOMEHERO_ENV + "" + str(salesforce_listing["backoffice_ID__c"]) + "", headers=HOMEHERO_AUTHORIZATION_HEADERS, data=json.dumps(backoffice_listing)) # TODO
    print(response.text) ####

    listing_backoffice_id = salesforce_listing["backoffice_ID__c"]
    contact_backoffice_id = str(int(salesforce_contact["backoffice_id__c"]))
    return (listing_backoffice_id, contact_backoffice_id)


def yad2_import_excel_to_sf(event, lambda_context):
    """
    AWS Lambda entry point, expects env variable SALESFORCE_DOMAIN, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN and SALESFORCE_USERNAME to contain salesforce account and HOMEHERO_ENV the env to sync to
    """
    try:

        # get s3 file link from event object
        print(event) ### for debugging
        path = "https://" + str(event["Records"][0]["s3"]["bucket"]["name"]) + ".s3-" + str(event["Records"][0]["awsRegion"]) + ".amazonaws.com/" + str(event["Records"][0]["s3"]["object"]["key"])
        
        # read ENV variables
        salesforce_domain           = str(os.environ['SALESFORCE_DOMAIN'])
        salesforce_password         = str(os.environ['SALESFORCE_PASSWORD'])
        salesforce_security_token   = str(os.environ['SALESFORCE_SECURITY_TOKEN'])
        salesforce_username         = str(os.environ['SALESFORCE_USERNAME'])
        
        global HOMEHERO_ENV
        HOMEHERO_ENV                = str(os.environ['HOMEHERO_ENV'])
        
        print("Yad2 Import Initialized for " + path + " to " + salesforce_username) ###
        teams_message("Yad2 Import Initialized for " + path + " to " + salesforce_username) ###
    
        df = prepare_data(path)
        sf = open_salesforce_connection(salesforce_domain, salesforce_password, salesforce_security_token, salesforce_username)
        send_listings(sf, df)
        
        print("Yad2 Import for " + path + " to " + salesforce_username + " done.") ###
        teams_message("Yad2 Import for " + path + " to " + salesforce_username + " done.") ###
        
        return {
            "statusCode": 200, 
            "body": json.dumps("Upload Sucessfull")
            }
        
    except Exception as e:
        
        print("Error: " + str(e))
        return {
            "statusCode": 400, 
            "body": json.dumps(str(e))
            }


#path                        = ""
#salesforce_domain           = ''
#salesforce_password         = ''
#salesforce_security_token   = ''
#salesforce_username         = ''
#df                          = prepare_data(path)
#sf                          = open_salesforce_connection(salesforce_domain, salesforce_password, salesforce_security_token, salesforce_username)
#send_listings(sf, df)
#print("done")
