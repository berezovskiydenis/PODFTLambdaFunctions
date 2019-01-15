import urllib.request
from datetime import datetime
from xml.etree import ElementTree as ET
import ssl

import boto3
from boto3.dynamodb.conditions import Key, Attr

# Links to files
# URL = "http://kfm.gov.kz/blacklist/export/excluded/xml"
URL = "https://kfm.gov.kz/blacklist/export/excluded/xml"

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
    # Request kfm web page
    req = urllib.request.urlopen(URL, context=ssl._create_unverified_context())
    # Create xml object
    xml_object = ET.fromstring(req.read().decode('utf-8'))

    items = []  # list of dictionaries containing persons or organizatons
    for prsn in xml_object.findall('persons/person'):
        items.append(create_person_item(prsn))

    return items


def lambda_handler(event, context):
    print('START')

    # Create list of dict that contains persons
    list_of_persons = build_items_from_XML()

    # Define DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
    table = dynamodb.Table('list_of_terrorists')

    # Loop through the list of persons
    for prsn in list_of_persons:

        response = table.get_item(
                Key = {'uuid': prsn['uuid']}
            )

        # Found. Update
        if response.get('Item') is not None:

            item = response['Item']
            items_list = item.get('note', [])
            excluded_list = prsn.get('note', [])

            if len(items_list) > 0 or len(excluded_list) > 0:
                new_list = items_list[:]
                new_list.extend(excluded_list)
                a_set = set(new_list)
                the_newest_list = list(a_set)

                table.update_item(
                        Key={
                            'uuid': prsn['uuid']
                        },
                        UpdateExpression='SET note = :val1',
                        ExpressionAttributeValues={
                            ':val1': the_newest_list
                        }
                    )
                print('Person {} {} ({}) updated'.format(prsn.get('lname'), prsn.get('fname'), prsn.get('iin')))
        else:
            table.put_item(
                    Item = prsn
                )
            print('Person {} {} ({}) created'.format(prsn.get('lname'), prsn.get('fname'), prsn.get('iin')))

    print('FINISH')
