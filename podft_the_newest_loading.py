import os
import http.cookiejar
import urllib.request
from datetime import datetime
from xml.etree import ElementTree as ET
import ssl
import boto3
from boto3.dynamodb.conditions import Key, Attr

# Links to files
# included_file = 'http://kfm.gov.kz/blacklist/export/included/xml'
included_file = 'https://kfm.gov.kz/blacklist/export/included/xml'

# Login and password
LOGIN = os.environ['KFM_LOGIN']
PASSWORD = os.environ['KFM_PASSWORD']

# A link to the login form
ACCESS_URL = 'http://kfm.gov.kz/assets/components/office/action.php?action=auth%2FformLogin&username={}&password={}&pageId=795'
ACCESS_URL = ACCESS_URL.format(LOGIN, PASSWORD)


def create_person_item(a_list):
    result = dict()

    # Loop over list of Elements. Every Element has 'tag' attribute and 'text'
    for value in a_list:
        if value.text is not None and len(value.text.strip()) > 0:
            if value.tag in ('lname', 'fname', 'mname', 'birthdate', 'iin'):
                result[value.tag] = value.text.strip().upper()
            elif value.tag in ('note', 'correction'):
                if result.get('note') is not None:
                    result['note'].append(value.text.strip().upper())
                else:
                    result['note'] = [value.text.strip().upper()]

    # Create UUID as a compound value
    if result.get('lname') is not None and result.get('iin') is not None:
        h = result.get('lname') + result.get('iin')
    elif result.get('lname') is not None and result.get('fname') is not None and result.get('mname') is not None:
        h = result.get('lname') + result.get('fname') + result.get('mname')
    elif result.get('lname') is not None and result.get('fname') is not None:
        h = result.get('lname') + result.get('fname')
    else:
        h = result.get('lname')
    result['uuid'] = h

    return result


def build_items_from_XML():
    cj = http.cookiejar.CookieJar()

    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ssl._create_unverified_context()), urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [('Cache-Control', 'no-cache')]
    req = opener.open(ACCESS_URL)
    r = opener.open(included_file)

    xml_object = ET.fromstring(r.read().decode('utf-8'))

    items = []  # list of dictionaries containing persons or organizatons

    for prsn in xml_object.findall('persons/person'):
        items.append(create_person_item(prsn))

    return items


def lambda_handler(event, context):

    print("START")

    # Create list of dict that contains persons
    list_of_persons = build_items_from_XML()
    print('Found {} persons'.format(len(list_of_persons)))

    # Define DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
    table = dynamodb.Table('list_of_terrorists')

    # Loop through the list of persons
    for prsn in list_of_persons:

        response = table.get_item(
                Key={'uuid': prsn['uuid']}
            )

        # If person is found then update note for a person
        if response.get('Item') is None:
            # Not found. Create new
            table.put_item(Item=prsn)
            print('{} {} ({}) not found. Created'.format(prsn.get('lname'), prsn.get('fname'), prsn.get('iin')))

    print("FINISH")