# Imports
import pandas as pd
import numpy as np
import simple_salesforce  # salesforce REST API client
import json
import re
import requests  # teams notification


# File Path
file_name = '' # TODO


# for failure detection
execution_state = "No mapping yet"


# Mapping Definitions
cities = {
    "פתח תקוה": ["Petah Tikva", 0],
    "כפר סבא": ["Kfar Saba", 1],
    "הרצליה": ["Hertzeliya", 2],
    "נתניה": ["Netanya", 3]
}


neighborhoods = [
    # Petah Tikva
    {
        "אחדות": "אחדות",
        "אם המושבות הותיקה": "אם המושבות",
        "אם המושבות החדשה": "הדר המושבות ",
        "מרכז": "מרכז העיר",
        "ביהח השרון": "מרכז העיר דרום",
        "בלינסון": "בלינסון",
        "כפר אברהם": "כפר אברהם",
        "כפר גנים": "כפר גנים",
        "כפר גנים א": "כפר גנים א",
        "כפר גנים ב": "כפר גנים ב",
        "כפר גנים ג": "כפר גנים ג",
        "רמת ורבר": "רמת ורבר",
        "בר יהודה": "בר יהודה",
        "חזני": "קרית חזני",
        "בת גנים": "בת גנים",
        "גני הדר": "גני הדר",
        "מחנה יהודה": "מחנה יהודה",
        "עמישב": "עמישב",
        "הדר גנים": "הדר גנים",
        "נוה גן": "נווה גן",
        "נוה גנים": "נווה גנים",
        "עין גנים": "עין גנים",
        "נוה עוז": "נווה עוז",
        "יוספטל": "יוספטל",
        "שעריה": "שערייה",
        "סגולה": "סגולה",
        "ק.אריה": "קרית אריה",
        "קרול": "קרול",
        "קרית אלון": "קרית אלון",
        "שיפר": "שיפר",
        "קרית מטלון": "קרית מטלון",
        "משהב": "משהב",
        "הפועל המזרחי": "שיכון פועלי המזרחי",
        "צ.סירקין": "צומת סירקין"
    },
    # Kfar Saba
    {
        "אזור התעשיה": "אזור התעשיה",
        "אזור תעשיה מזרח": "אזור תעשיה מזרח",
        "אלי כהן": "אלי כהן",
        "אליעזר": "אליעזר",
        "בית ונוף": "בית ונוף",
        "גאולים": "גאולים",
        "גבעת אשכול": "גבעת אשכול ( קדמת הדרים)",
        "תשלוז": "גני השרון - תשלו\"ז",
        "גני השרון": "גני השרון - תשלו\"ז",
        "הדרים הותיקה": "שכונת הדרים הוותיקה",
        "הדרים החדשה": "שכונת הדרים החדשה",
        "הזמר העברי": "כפר סבא הירוקה-דרום (הירוקה 60)",
        "הירוקה": "הירוקה",
        "הפרחים": "שכונת הפרחים / דגניה",
        "הצעירה": "כפר סבא הצעירה/ האוניברסיטה",
        "קאנטרי": "כפר סבא הצעירה/ האוניברסיטה",
        "הראשונים": "הראשונים",
        "ותיקים": "שיכון וותיקים",
        "יוספטל": "יוספטל",
        "כיסופים": "כיסופים",
        "מרכז": "מרכז העיר",
        "משה\"ב": "משה דיין צפון - משה\"ב",
        "סביוני הכפר": "סביוני הכפר",
        "סירקין": "סירקין",
        "עליה": "שיכון עלייה",
        "קפלן": "קפלן",
        "שכונת הפארק": "שכונת הפארק",
        "תקומה": "תקומה",
        "צומת כפר סבא": "צומת כפר סבא"
    },
    # Herzliya
    {
        "גן רשל": "גן רש\"ל",
        "ב'": "הרצליה ב",
        "הירוקה": "הרצליה הירוקה",
        "הצעירה": "הרצליה הצעירה",
        "פיתוח": "הרצליה פיתוח",
        "חוף התכלת": "הרצליה פיתוח",
        "ויצמן": "וייצמן",
        "נווה אמירים": "נווה אמירים",
        "נווה ישראל": "נווה ישראל",
        "נווה עמל": "נווה עמל",
        "נוף ים": "נוף ים",
        "נחלת עדה": "נחלת עדה",
        "מרכז": "מרכז העיר",
        "שביב": "יד התשעה",
        "שכונת צמרות": "צמרות",
        "מרינה": "מרינה",
        "גליל ים": "גליל ים"
    },
    # Netanya
    {
        "אגמים": "אגמים",
        "א.ת.ישן": "אזור התעשייה קריית אליעזר (אזור תעשיה ישן, קריית אליעזר)",
        "פארק ספיר": "אזור תעשייה - ספיר",
        "בן ציון": "בן ציון",
        "ג.האירוסים": "גבעת האירוסים",
        "גלי ים": "גלי הים",
        "מרכז": "מרכז העיר",
        "משכנות זבולון": "משכנות זבולון",
        "ותיקים": "נאות גנים",
        "רמת הרצל": "נאות הרצל",
        "עמידר": "נאות הרצל",
        "סלע": "נאות הרצל",
        "נאות שקד": "נאות שקד",
        "אזורים": "נאות שקד",
        "נוה איתמר": "נוה איתמר",
        "נוה שלום": "נווה שלום",
        "נאות התכלת": "נווה שלום",
        "נת600": "נת/600, נוף הטיילת",
        "נוף הטיילת": "נת/600, נוף הטיילת",
        "עין התכלת": "עין התכלת",
        "עיר ימים": "עיר ימים",
        "פרדס הגדוד": "פרדס הגדוד",
        "ק.נורדאו": "קרית נורדאו",
        "רמת אפרים": "רמת אפרים",
        "רמת חן": "רמת חן",
        "דורה": "רמת יגאל ידין - דורה",
        "רמת פולג": "רמת פולג",
        "ק.השרון": "קריית השרון",
        "ק.רבין": "קריית יצחק רבין",
        "דרום": "דרום מרכז העיר",
        "צפון": "צפון מרכז העיר",
        "נוף גלים": "נוף גלים",
        "גבעת הפרחים": "אחר",
        "נוה פולג": "אחר",
        "טוברוק": "טוברוק"
    }
]


types = {
    "דירה": "Apartment",
    "ד.מרתף": "Ground Apartment",
    "דירת גן": "Garden Apartment",
    "ד.גג": "Penthouse/Rooftop Apartment",
    "מיני פנט'": "Mini Penthouse",
    "פנטהאוס": "Penthouse/Rooftop Apartment",
    "'קוטג": "Cottage",
    "מיני קוטג": "Cottage",
    "דו משפחתי": "Two Family Cottage",
    "בית,דו משפחתי": "Two Family Cottage",
    "וילה": "Villa",
    "בית": "House",
    "דופלקס": "Duplex",
    "טריפלקס": "Triplex",
    "לופט": "Studio / loft",
    "סטודיו": "Studio / loft",
    "יח. דיור": "Residential Unit",
    "משק/נחלה": "Other",
    "מגרש": "Plot",
    "רביעיות": "Other",
    "להשקעה": "Investment Apartment",
    "משק עזר": "Other",
    "אחר": "Other"
}


def elevator_mapping(current_value):
    if current_value == 'לא':
        return '0'
    try:
        new_value = re.search('כן', current_value).group(1)
        return '1'
    except AttributeError:
        # no match found in the original string
        return '0'
    except IndexError:
        # no match found in the original string
        return '0'


balcony_mapping = {
    'אין': '0',
    'יש': '1'
}


def getCityName(city):
    try:
        return cities[city][0]
    except:
        return ""


def getCityID(city):
    try:
        return cities[city][1]
    except:
        return -1


def getNeighborhoodName(city, neighborhood):
    index = getCityID(city)
    if index == -1:
        return ""
    try:
        return neighborhoods[index][neighborhood]
    except:
        if neighborhood != '':
            messageToTeams('New Webtiv neighborhood detected: ' + str(neighborhood))
        return "אחר"


# import excel file into Dataframe and format all fields according to Salesforce
def prepare_data():

    # read file into a DataFrame
    df = pd.read_excel(file_name, na_filter=False)
    messageToTeams('Webtiv Import for file ' + str(file_name) + ' has started.')
    global execution_state

    # Webtiv_Id column -> -> webtiv_ID__c column
    df.rename(columns={'Webtiv_Id': 'webtiv_ID__c'}, inplace=True)
    df['webtiv_ID__c'] = df['webtiv_ID__c'].astype(str)
    execution_state = 'Webtiv_Id -> webtiv_ID__c mapping completed'

    # external_agent_name column -> -> webtiv_External_Agent_Name__c column
    df.rename(columns={'external_agent_name': 'webtiv_External_Agent_Name__c'}, inplace=True)
    df['webtiv_External_Agent_Name__c'] = df['webtiv_External_Agent_Name__c'].astype(str)
    execution_state = 'external_agent_name -> webtiv_External_Agent_Name__c mapping completed'

    # phone_number column -> -> Mobile__c column
    df.rename(columns={'phone_number': 'Mobile__c'}, inplace=True)
    df['Mobile__c'] = df['Mobile__c'].astype(str)
    df['Mobile__c'] = df['Mobile__c'].replace('-', '', regex=True)
    execution_state = 'phone_number -> Mobile__c mapping completed'

    # apartment_type column -> -> pba__PropertyType__c column
    df.rename(columns={'apartment_type': 'pba__PropertyType__c'}, inplace=True)
    df['pba__PropertyType__c'] = df['pba__PropertyType__c'].astype(str)
    df['pba__PropertyType__c'] = df['pba__PropertyType__c'].map(lambda webtiv_type: types.get(webtiv_type, "אחר"))
    execution_state = 'apartment_type -> pba__PropertyType__c mapping completed'

    # rooms column -> -> Room_HH__c column
    df.rename(columns={'rooms': 'Room_HH__c'}, inplace=True)
    df['Room_HH__c'] = df['Room_HH__c'].astype(np.float64)
    df['Room_HH__c'] = df['Room_HH__c'].round()
    df['Room_HH__c'] = df['Room_HH__c'].astype("Int64")
    df['Room_HH__c'] = df['Room_HH__c'].astype(str)
    execution_state = 'rooms -> Room_HH__c mapping completed'

    # price column -> -> pba__ListingPrice_pb__c column
    df.rename(columns={'price': 'pba__ListingPrice_pb__c'}, inplace=True)
    df['pba__ListingPrice_pb__c'] = df['pba__ListingPrice_pb__c'].astype(str)
    execution_state = 'price -> pba__ListingPrice_pb__c mapping completed'

    # neighborhood column -> -> Neighborhoods_HH__c column
    df.rename(columns={'neighborhood': 'Neighborhoods_HH__c'}, inplace=True)
    df['Neighborhoods_HH__c'] = df['Neighborhoods_HH__c'].astype(str)
    for row_index in df.Neighborhoods_HH__c.index:
        df.at[row_index, 'Neighborhoods_HH__c'] = getNeighborhoodName(df.at[row_index, 'city'], df.at[row_index, 'Neighborhoods_HH__c'])
    execution_state = 'neighborhood -> Neighborhoods_HH__c mapping completed'

    # city column -> -> Property_City__c column
    df.rename(columns={'city': 'Property_City__c'}, inplace=True)
    df['Property_City__c'] = df['Property_City__c'].astype(str)
    df['Property_City__c'] = df['Property_City__c'].map(lambda webtiv_city_name: getCityName(webtiv_city_name))
    execution_state = 'city -> Property_City__c mapping completed'

    # street column / house_number column -> pba__Address_pb__c column
    df['street'] = df['street'].astype(str)
    df['house_number'] = df['house_number'].astype(str)
    df['pba__Address_pb__c'] = ''
    df['pba__Address_pb__c'] = df['pba__Address_pb__c'].astype(str)
    df['pba__Address_pb__c'] = df['street'] + " " + df['house_number']
    execution_state = 'street / house_number -> pba__Address_pb__c mapping completed'

    # floor column -> -> pba__Floor__c column
    df.rename(columns={'floor': 'pba__Floor__c'}, inplace=True)
    df['pba__Floor__c'] = df['pba__Floor__c'].astype(str)
    execution_state = 'floor -> pba__Floor__c mapping completed'

    # elevator column -> -> Elavator__c column
    df.rename(columns={'elevator': 'Elavator__c'}, inplace=True)
    df['Elavator__c'] = df['Elavator__c'].astype(str)
    df['Elavator__c'] = df['Elavator__c'].map(elevator_mapping)
    execution_state = 'elevator -> Elavator__c mapping completed'

    # created column -> -> webtiv_Created_Date__c column
    df.rename(columns={'created': 'webtiv_Created_Date__c'}, inplace=True)
    df['webtiv_Created_Date__c'] = df['webtiv_Created_Date__c'].astype(str)
    df['created2'] = df['webtiv_Created_Date__c']
    execution_state = 'created -> webtiv_Created_Date__c mapping completed'

    # created2 column -> -> Created_Date__c column
    df.rename(columns={'created2': 'Created_Date__c'}, inplace=True)
    df['Created_Date__c'] = df['Created_Date__c'].astype(str)
    df['Created_Date__c'] = df['Created_Date__c'] + 'T00:00:00.000+0300'
    execution_state = 'created2 -> Created_Date__c mapping completed'

    # updated column -> -> Last_communication_on_Webtiv__c column
    df.rename(columns={'updated': 'Last_communication_on_Webtiv__c'}, inplace=True)
    df['Last_communication_on_Webtiv__c'] = df['Last_communication_on_Webtiv__c'].astype(str)
    execution_state = 'updated -> Last_communication_on_Webtiv__c mapping completed'

    # balcony column -> -> Balcony__c column
    df.rename(columns={'balcony': 'Balcony__c'}, inplace=True)
    df['Balcony__c'] = df['Balcony__c'].astype(str)
    df['Balcony__c'] = df['Balcony__c'].map(balcony_mapping)
    execution_state = 'balcony -> Balcony__c mapping completed'

    # square_meters column -> -> pba__SqFt_pb__c column
    df.rename(columns={'square_meters': 'pba__SqFt_pb__c'}, inplace=True)
    df['pba__SqFt_pb__c'] = df['pba__SqFt_pb__c'].astype(str)
    execution_state = 'square_meters -> pba__SqFt_pb__c mapping completed'

    #print('Webtiv -> Salesforce data mapping completed')
    return df


# open session to CRM API user
def open_salesforce_connection():
    session_id, instance = simple_salesforce.SalesforceLogin(username='',
                                                             password='',
                                                             security_token='')
    sf = simple_salesforce.Salesforce(instance=instance, session_id=session_id)
    return sf


# create listing / update listing in Salesforce row by row through Dataframe
def send_listings(sf, df):
    global execution_state
    execution_state = 'No listings imported yet'
    for row_index in df.webtiv_ID__c.index:
        existingListing = sf.query("SELECT id FROM pba__Listing__c WHERE webtiv_ID__c = '" + df.at[row_index, 'webtiv_ID__c'] + "' LIMIT 1")
        try:
            existingListingId = existingListing['records'][0]['Id']
            sf.pba__Listing__c.update(str(existingListingId), {
                'webtiv_ID__c':                     df.at[row_index, 'webtiv_ID__c'],
                'Mobile__c':                        df.at[row_index, 'Mobile__c'],
                'pba__PropertyType__c':             df.at[row_index, 'pba__PropertyType__c'],
                'Room_HH__c':                       df.at[row_index, 'Room_HH__c'],
                'pba__ListingPrice_pb__c':          df.at[row_index, 'pba__ListingPrice_pb__c'],
                'pba__OriginalListPrice_pb__c':     df.at[row_index, 'pba__ListingPrice_pb__c'],
                'Property_City__c':                 df.at[row_index, 'Property_City__c'],
                'pba__Address_pb__c':               df.at[row_index, 'pba__Address_pb__c'],
                'pba__Floor__c':                    df.at[row_index, 'pba__Floor__c'],
                'Elavator__c':                      df.at[row_index, 'Elavator__c'],
                'webtiv_Created_Date__c':           df.at[row_index, 'webtiv_Created_Date__c'],
                'Created_Date__c':                  df.at[row_index, 'Created_Date__c'],
                'Last_communication_on_Webtiv__c':  df.at[row_index, 'Last_communication_on_Webtiv__c'],
                'Neighborhoods_HH__c':              df.at[row_index, 'Neighborhoods_HH__c'],
                'Balcony__c':                       df.at[row_index, 'Balcony__c'],
                'pba__SqFt_pb__c':                  df.at[row_index, 'pba__SqFt_pb__c'],
                'Source__c':                        'webtiv',
                'Owning_work_on_the_property__c':   'Collaboration',
                'Exclusivity_other_agent__c':       'true',
                'Seller_side_commission_signed__c': '0',
                'pba__Status__c':                   'Exclusively',
                'type__c':                          'External'
            })
            message = (
                 'Existing Listing with ID ' + str(existingListingId) + ' updated in Salesforce to: '
                 'webtiv_ID__c: ' + df.at[row_index, 'webtiv_ID__c'] + ', '
                 'Mobile__c: ' + df.at[row_index, 'Mobile__c'] + ', '
                 'pba__PropertyType__c: ' + df.at[row_index, 'pba__PropertyType__c'] + ', '
                 'Room_HH__c: ' + df.at[row_index, 'Room_HH__c'] + ', '
                 'pba__ListingPrice_pb__c & pba__OriginalListPrice_pb__c: ' + df.at[row_index, 'pba__ListingPrice_pb__c'] + ', '
                 'Property_City__c: ' + df.at[row_index, 'Property_City__c'] + ', '
                 'pba__Address_pb__c: ' + df.at[row_index, 'pba__Address_pb__c'] + ', '
                 'pba__Floor__c: ' + df.at[row_index, 'pba__Floor__c'] + ', '
                 'Elavator__c: ' + df.at[row_index, 'Elavator__c'] + ', '
                 'webtiv_Created_Date__c: ' + df.at[row_index, 'webtiv_Created_Date__c'] + ', '
                 'Created_Date__c: ' + df.at[row_index, 'Created_Date__c'] + ', '
                 'Last_communication_on_Webtiv__c' +  df.at[row_index, 'Last_communication_on_Webtiv__c'] + ', '
                 'Neighborhoods_HH__c: ' + df.at[row_index, 'Neighborhoods_HH__c'] + ', '
                 'Balcony__c: ' + df.at[row_index, 'Balcony__c'] + ', '
                 'pba__SqFt_pb__c: ' + df.at[row_index, 'pba__SqFt_pb__c'] + ', '
                 'Source__c: webtiv, '
                 'Owning_work_on_the_property__c: Collaboration, '
                 'Exclusivity_other_agent__c: true, '
                 'Seller_side_commission_signed__c: 0, '
                 'pba__Status__c: Exclusively, '
                 'type__c: External'
            )
            execution_state = str(existingListingId)
            #print(message)
        except IndexError:
            newContact = sf.Contact.create({
                'First_Name_HH__c':                 getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[0],
                'FirstName':                        getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[0],
                'LastName':                         getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[1],
                'lastNameForFormula__c':            getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[1],
                'pba__ContactType_pb__c':           'Real Estate Agent',
                'MobilePhone':                      df.at[row_index, 'Mobile__c'],
                'OwnerId':                          '0054J000002EgHFQA0',
            })
            newListing = sf.pba__Listing__c.create({
                'webtiv_ID__c':                     df.at[row_index, 'webtiv_ID__c'],
                'Mobile__c':                        df.at[row_index, 'Mobile__c'],
                'pba__PropertyType__c':             df.at[row_index, 'pba__PropertyType__c'],
                'Room_HH__c':                       df.at[row_index, 'Room_HH__c'],
                'pba__ListingPrice_pb__c':          df.at[row_index, 'pba__ListingPrice_pb__c'],
                'pba__OriginalListPrice_pb__c':     df.at[row_index, 'pba__ListingPrice_pb__c'],
                'Property_City__c':                 df.at[row_index, 'Property_City__c'],
                'pba__Address_pb__c':               df.at[row_index, 'pba__Address_pb__c'],
                'pba__Floor__c':                    df.at[row_index, 'pba__Floor__c'],
                'Elavator__c':                      df.at[row_index, 'Elavator__c'],
                'webtiv_Created_Date__c':           df.at[row_index, 'webtiv_Created_Date__c'],
                'Created_Date__c':                  df.at[row_index, 'Created_Date__c'],
                'Last_communication_on_Webtiv__c':  df.at[row_index, 'Last_communication_on_Webtiv__c'],
                'Neighborhoods_HH__c':              df.at[row_index, 'Neighborhoods_HH__c'],
                'Balcony__c':                       df.at[row_index, 'Balcony__c'],
                'pba__SqFt_pb__c':                  df.at[row_index, 'pba__SqFt_pb__c'],
                'Source__c':                        'webtiv',
                'OwnerId':                          '0054J000002EgHFQA0',
                'Owning_work_on_the_property__c':   'Collaboration',
                'pba__PropertyOwnerContact_pb__c':  str(newContact['id']),
                'Exclusivity_other_agent__c':       'true',
                'Seller_side_commission_signed__c': '0',
                'pba__Status__c':                   'Exclusively',
                'type__c':                          'External'
            })
            newListingInfo = sf.query("SELECT pba__Property__c FROM pba__Listing__c WHERE ID = '" + str(newListing['id']) + "' LIMIT 1")
            sf.pba__PropertyMedia__c.create({
                'pba__Category__c': 'Images',
                'pba__Property__c': str(newListingInfo['records'][0]['pba__Property__c']),
                'pba__IsExternalLink__c': 'true',
                'pba__ExternalLink__c': '', # TODO
                'pba__IsOnExpose__c': 'true'
            })
            sf.Contact.update(str(newContact['id']), {
                'Assigned_Property__c': str(newListing['id'])
            })
            message = (
                    'New Listing with ID ' + str(newListing['id']) + ' entered into Salesforce: '
                    'webtiv_ID__c: ' + df.at[row_index, 'webtiv_ID__c'] + ', '
                    'Mobile__c: ' + df.at[row_index, 'Mobile__c'] + ', '
                    'pba__PropertyType__c: ' + df.at[row_index, 'pba__PropertyType__c'] + ', '
                    'Room_HH__c: ' + df.at[row_index, 'Room_HH__c'] + ', '
                    'pba__ListingPrice_pb__c & pba__OriginalListPrice_pb__c: ' + df.at[row_index, 'pba__ListingPrice_pb__c'] + ', '
                    'Property_City__c: ' + df.at[row_index, 'Property_City__c'] + ', '
                    'pba__Address_pb__c: ' + df.at[row_index, 'pba__Address_pb__c'] + ', '
                    'pba__Floor__c: ' + df.at[row_index, 'pba__Floor__c'] + ', '
                    'Elavator__c: ' + df.at[row_index, 'Elavator__c'] + ', '
                    'webtiv_Created_Date__c: ' + df.at[row_index, 'webtiv_Created_Date__c'] + ', '
                    'Created_Date__c: ' + df.at[row_index, 'Created_Date__c']+ ', '
                    'Last_communication_on_Webtiv__c' +  df.at[row_index, 'Last_communication_on_Webtiv__c'] + ', '
                    'Neighborhoods_HH__c: ' + df.at[row_index, 'Neighborhoods_HH__c'] + ', '
                    'Balcony__c: ' + df.at[row_index, 'Balcony__c'] + ', '
                    'pba__SqFt_pb__c: ' + df.at[row_index, 'pba__SqFt_pb__c'] + ', '
                    'Source__c: webtiv , '
                    'OwnerId: 0054J000002EgHFQA0, '
                    'Owning_work_on_the_property__c: Collaboration, '
                    'pba__PropertyOwnerContact_pb__c: ' + str(newContact['id']) + ', '
                    'Exclusivity_other_agent__c: true, '
                    'Seller_side_commission_signed__c: 0, '
                    'pba__Status__c: Exclusively, '
                    'type__c: External, '
                    'New Real Estate Agent Contact with ID ' + str(newContact['id']) + ' entered into Salesforce: '
                    'First_Name_HH__c & FirstName: ' + getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[0] + ', '
                    'LastName & lastNameForFormula__c: ' + getName(df.at[row_index, 'webtiv_External_Agent_Name__c'])[0] + ', '
                    'pba__ContactType_pb__c: Real Estate Agent, '
                    'MobilePhone: ' + df.at[row_index, 'Mobile__c'] + ', '
                    'OwnerId: 0054J000002EgHFQA0'
            )
            execution_state = str(newListing['id'])
            #print(message)


def getName(full_name):
    split_name = full_name.strip().rsplit(" ", 1)
    if split_name == ['']:
        return ["unknown", "unknown"]
    elif len(split_name) == 1:
        return ["unknown", split_name[0]]
    else:
        return split_name


def messageToTeams(message):
    response = requests.post(
        '', # TODO
        data=json.dumps({"message": message}),
        headers={'content-type': 'application/json'}
    )
    return None


def handleExcel(event, lambda_context):
    global execution_state
    data_transformed = False
    connection_established = False
    try:
        df = prepare_data()
        data_transformed = True
        sf = open_salesforce_connection()
        connection_established = True
        send_listings(sf, df)
        messageToTeams('Webtiv Import all listings added to Salesforce')
    except:
        if not data_transformed:
            messageToTeams('Webtiv -> Salesforce data mapping could not be completed, last performed action is: ' + execution_state)
        elif connection_established:
            messageToTeams('Webtiv Import failed, last imported listing: ' + execution_state)
        else:
            messageToTeams('Webtiv Import failed because connection to Salesforce could not be established')
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({
            "message": "Completed Successfully"
        }),
    }
