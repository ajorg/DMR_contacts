#!/usr/bin/env python2
import csv
import json
import re
from urllib2 import urlopen

# The JSON is invalid, because of mixed encodings. The CSV also has
# data quality issues, but most can be ignored.
DB_URL = ('http://www.dmr-marc.net/cgi-bin/trbo-database/datadump.cgi'
          '?table=users&format=csv&header=1')

# The CS750 uses a 6-bit encoding for the Call Alias, using only letters,
# numbers, space, and period.
# Dash would have been a better choice than period, but the decision was made
# without looking closely at the data.
ALIAS_ILLEGAL = re.compile('[^a-zA-Z0-9\. ]')

# These are the field names as given in a CS750 contacts export.
FIELDNAMES = ('No', 'Call Alias', 'Call Type', 'Call ID', 'Receive Tone')


def alias_user(user):
    """Takes a user record as given in the DMR-MARC csv and returns a
    Call Alias composed of the Callsign and either the Nickname or Name.
    """
    callsign = user['Callsign']

    name = user['Nickname']
    if name:
        name = name.strip()

    names = user['Name']
    if names:
        names = names.split()

    # name is still Nickname here. If it's None or empty, use the first name.
    if not name and names:
        name = names[0]
        # Skip titles. Have not seen other titles in the data yet.
        if name.lower() in ('dr', 'dr.'):
            name = names[1]

    if name:
        alias = ' '.join((callsign, name))
    else:
        # Both the Nickname and Name fields could be empty.
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


def read_users_csv(users):
    """Reads DMR-MARC csv from the users file-like object and returns a list of
    dicts in CS750 export format."""
    csvr = csv.DictReader(users)
    result = []
    for row in csvr:
        try:
            result.append({
                'Call Alias': alias_user(row),
                'Call Type': 'Private Call',
                'Call ID': row['Radio ID'],
                'Receive Tone': 'No'})
        except TypeError:
            # For now, skip records that have problems. The most common is an
            # empty record, because of a newline in a field.
            pass
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
    row = 1
    for contact in contacts:
        col = 0
        for field in fieldnames:
            if field in contact:
                ws.write(row, col, contact[field])
            col += 1
        row += 1
    wb.close()


def get_users(db_url=DB_URL):
    db = urlopen(DB_URL)
    users = read_users_csv(db)
    db.close()
    return users


def get_groups(sources=('dci-groups.json',)):
    groups = []
    for source in sources:
        with open(source) as s:
            groups.extend(read_groups_json(s))
    return groups


if __name__ == '__main__':
    users = get_users()
    groups = get_groups()

    with open('contacts-dci.xlsx', 'wb') as xlsxo:
        write_contacts_xlsx(groups + users, xlsxo)

    # In exported contacts, the sheet name is DMR_contacts. Naming the file
    # this way maintains that, though it seems to not be important.
    with open('DMR_contacts.csv', 'wb') as csvo:
        write_contacts_csv(users, csvo)
