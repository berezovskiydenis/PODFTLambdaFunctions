import urllib.request
from datetime import datetime
from xml.etree import ElementTree as ET
import ssl
import boto3

# Links to files
# URL = "http://kfm.gov.kz/blacklist/export/active/xml"
URL = "https://kfm.gov.kz/blacklist/export/active/xml"


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
    print('... processed {}'.format(h))
    return result


def build_items_from_XML():
    # Request kfm web page
    req = urllib.request.urlopen(URL, context=ssl._create_unverified_context())
    # Create xml object
    xml_object = ET.fromstring(req.read().decode('utf-8'))

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

    with table.batch_writer() as batch:
        for prsn in list_of_persons:
            batch.put_item(Item=prsn)

    print("FINISH")
