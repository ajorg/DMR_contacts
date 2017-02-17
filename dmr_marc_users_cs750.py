#!/usr/bin/env python
import csv
import json
import logging
import re
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
from io import BytesIO

import requests
import boto3

s3 = boto3.client('s3')

logger = logging.getLogger(__name__)

# Woohoo! The JSON dump seems to be valid now!
USERS_URL = 's3://dmr-contacts/marc/users.json'
GROUPS_URL = 's3://dmr-contacts/brandmeister/groups.json'

# The CS750 uses a 6-bit encoding for the Call Alias, using only letters,
# numbers, space, and period.
# Dash would have been a better choice than period, but the decision was made
# without looking closely at the data.
ALIAS_ILLEGAL = re.compile('[^a-zA-Z0-9\. ]')

# These are the field names as given in a CS750 contacts export.
FIELDNAMES = ('No', 'Call Alias', 'Call Type', 'Call ID', 'Receive Tone')

ALL_CALL = [{
    'Call Alias': 'All Call',
    'Call Type': 'All Call',
    'Call ID': 1677215,
    'Receive Tone': 'Yes',
    }]
SIMPLEX = [{
    'Call Alias': 'Simplex',
    'Call Type': 'Group Call',
    'Call ID': 99,
    'Receive Tone': 'No',
    }]

JSON_DICT = re.compile(r'({.*})', re.DOTALL)


def js_json(js):
    """Unwraps JSON from the JavaScript containing it."""
    return JSON_DICT.search(js).groups()[0]


def alias_user(user):
    """Takes a user record as given in the DMR-MARC json and returns a
    Call Alias composed of the callsign and name.
    """
    callsign = user['callsign']
    logger.debug(callsign)

    names = user.get('name').split()
    # Some names are just " "?
    if names:
        name = names[0]
        # Skip titles. Have not seen other titles in the data yet.
        if name.lower() in ('dr', 'dr.') and len(names) > 1:
            name = names[1]
        alias = ' '.join((callsign, name))
    else:
        alias = callsign

    # Could do something more useful, like transliterating, but this will
    # require detecting the encoding, which varies from record to record.
    return ALIAS_ILLEGAL.sub('', alias)


def alias_group(group):
    """Takes a talkgroup record in an intermediate format and returns an alias
    with the timeslot number appended if not already represented.

    The intention is to make it easier to apply the correct timeslot when
    creating a channel in the CPS.

    Input:
    {
      "timeslot": 2,
      "name": "Utah",
    }

    Output:
    "Utah 2"
    """
    alias = None
    if not isinstance(group, dict):
        group = {'name': group}
    if 'timeslot' not in group or not group['timeslot']:
        alias = group['name']
    else:
        timeslot = str(group['timeslot'])
        name = group['name']
        # Only if it ends in ' n', or we won't append to 'TAC 312'
        if name[-2:] == ' ' + timeslot:
            alias = name
        else:
            alias = ' '.join((name, timeslot))
    return ALIAS_ILLEGAL.sub('', alias)


def read_users(users):
    """Reads DMR-MARC csv from the users file-like object and returns a list of
    dicts in CS750 export format."""
    result = []
    for user in users:
        try:
            result.append({
                'Call Alias': alias_user(user),
                'Call Type': 'Private Call',
                'Call ID': int(user['radio_id']),
                'Receive Tone': 'No'})
        except TypeError as e:
            # For now, skip records that have problems. The most common is an
            # empty record, because of a newline in a field.
            logger.debug(e.message)
    return sorted(result, key=lambda k: k['Call ID'])


def read_groups_json(groups):
    """Reads json from the groups file-like object and returns a list of dicts
    in CS750 export format."""
    result = []
    for tgid, group in json.load(groups).items():
        result.append({
            'Call Alias': alias_group(group),
            'Call Type': 'Group Call',
            'Call ID': tgid,
            'Receive Tone': 'No'})
    return sorted(result, key=lambda k: k['Call Alias'])


def write_contacts_csv(contacts, csvo, fieldnames=FIELDNAMES):
    """Writes contacts (expected in CS750 format) to the csvo file-like object.
    """
    csvw = csv.DictWriter(csvo, fieldnames)
    csvw.writeheader()
    for row in contacts:
        csvw.writerow(row)


def write_contacts_xlsx(contacts, xlsxo,
                        fieldnames=FIELDNAMES, worksheet='DMR_contacts'):
    """Writes contacts (expected in CS750 format) to the xlsxo file-like
    object."""
    import xlsxwriter
    wb = xlsxwriter.Workbook(xlsxo, {'in_memory': True})
    ws = wb.add_worksheet(worksheet)
    col = 0
    for field in fieldnames:
        ws.write_string(0, col, field)
        col += 1
    seen = {}
    row = 1
    for contact in contacts:
        if contact['Call ID'] in seen:
            logger.debug('DUP! %s / %s',
                         contact['Call Alias'], seen[contact['Call ID']])
            continue
        else:
            seen[contact['Call ID']] = contact['Call Alias']
        col = 0
        for field in fieldnames:
            if field in contact:
                if field == 'Call ID':
                    ws.write(row, col, int(contact[field]))
                else:
                    ws.write(row, col, contact[field])
            col += 1
        row += 1
    wb.close()


def get_users(users_url=USERS_URL):
    parsed = urlparse(users_url)
    if parsed.scheme == 's3':
        o = s3.get_object(Bucket=parsed.netloc,
                          Key=parsed.path.lstrip('/'))
        data = o.get('Body').read()
    else:
        db = requests.get(users_url)
        data = db.content
    data = data.decode('utf-8', 'replace').encode('ascii', 'replace')
    users = json.loads(data)['users']
    return read_users(users)


def get_groups_bm(groups_url=GROUPS_URL):
    parsed = urlparse(groups_url)
    if parsed.scheme == 's3':
        o = s3.get_object(Bucket=parsed.netloc,
                          Key=parsed.path.lstrip('/'))
        data = o.get('Body').read().decode('utf-8')
        bm_json = StringIO(data)
    else:
        bm = requests.get(groups_url)
        bm_json = StringIO(js_json(bm.text))
    groups = read_groups_json(bm_json)
    bm_json.close()
    return groups


def s3_contacts(contacts, bucket, key):
    s3 = boto3.client('s3')

    o = BytesIO()

    if key.endswith('.csv'):
        t = 'text/csv'
        if key.startswith('N0GSG/'):
            write_n0gsg_csv(contacts, o)
        else:
            write_contacts_csv(contacts, o)
    elif key.endswith('.xlsx'):
        t = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        write_contacts_xlsx(contacts, o)

    s3.put_object(
        Bucket=bucket, Key=key, StorageClass='REDUCED_REDUNDANCY',
        Body=o.getvalue(), ContentType=t, ACL='public-read')
    o.close()


def lambda_handler(event=None, context=None):
    logger.info('Getting MARC Users')
    marc = get_users()
    logger.info('Getting BrandMeister Groups')
    bm = get_groups_bm()

    logger.info('Writing CS750 Contacts .csv')
    s3_contacts(contacts=marc, bucket='dmr-contacts',
                key='CS750/DMR_contacts.csv')

    logger.info('Writing CS750 BrandMeister + MARC .xlsx')
    s3_contacts(contacts=SIMPLEX + bm + marc, bucket='dmr-contacts',
                key='CS750/brandmeister-marc.xlsx')


if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    marc = get_users()
    bm = get_groups_bm()

    with open('DMR_contacts.xlsx', 'wb') as xlsxo:
        write_contacts_xlsx(SIMPLEX + bm + marc, xlsxo)

    # In exported contacts, the sheet name is DMR_contacts. Naming the file
    # this way maintains that, though it seems to not be important.
    with open('DMR_contacts.csv', 'w') as csvo:
        write_contacts_csv(SIMPLEX + bm + marc, csvo)
