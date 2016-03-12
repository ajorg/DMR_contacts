#!/usr/bin/env python2
import csv
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
ILLEGAL = re.compile('[^a-zA-Z0-9\. ]')

# These are the field names as given in a CS750 contacts export.
FIELDNAMES = ('No', 'Call Alias', 'Call Type', 'Call ID', 'Receive Tone')


def alias(user):
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
    return ILLEGAL.sub('', alias)


def read_users_csv(users):
    """Reads DMR-MARC csv from the db file-like object and returns a list of
    dicts in CS750 export format."""
    csvr = csv.DictReader(db)
    result = []
    for row in csvr:
        try:
            result.append({
                'Call Alias': alias(row),
                'Call Type': 'Private Call',
                'Call ID': row['Radio ID'],
                'Receive Tone': 'No'})
        except TypeError:
            # For now, skip records that have problems. The most common is an
            # empty record, because of a newline in a field.
            pass
    return result


def write_contacts_csv(data, csvo, fieldnames=FIELDNAMES):
    """Writes the data (expected in CS750 format) to the csvo file-like object.
    """
    csvw = csv.DictWriter(csvo, fieldnames)
    csvw.writeheader()
    for row in data:
        csvw.writerow(row)


if __name__ == '__main__':
    # In exported contacts, the sheet name is DMR_contacts. Naming the file
    # this way maintains that, though it seems to not be important.
    with open('DMR_contacts.csv', 'wb') as csvo:
        db = urlopen(DB_URL)
        write_contacts_csv(read_users_csv(db), csvo)
        db.close()
