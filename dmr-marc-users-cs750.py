#!/usr/bin/env python2
import csv
import re
import sys
from urllib2 import urlopen

DB_URL = 'http://www.dmr-marc.net/cgi-bin/trbo-database/datadump.cgi?table=users&format=csv&header=1'
ILLEGAL = re.compile('[^a-zA-Z0-9\. ]')
FIELDNAMES = ('No', 'Call Alias', 'Call Type', 'Call ID', 'Receive Tone')

def alias(user):
    callsign = user['Callsign']
    name = user['Nickname']
    if name:
        name = name.strip()
    names = user['Name']
    if names:
        names = names.split()

    if not name and names:
	name = names[0]
        if name in ['Dr', 'Dr.']:
            name = names[1]
    if name:
        alias = ' '.join((callsign, name))
    else:
        alias = callsign
    return ILLEGAL.sub('', alias)
    

db = urlopen(DB_URL)
csvr = csv.DictReader(db)

with open('DMR_contacts.csv', 'wb') as f:
    csvw = csv.DictWriter(f, fieldnames=FIELDNAMES)
    csvw.writeheader()
    for row in csvr:
        try:
            csvw.writerow({
                'Call Alias': alias(row),
                'Call Type': 'Private Call',
                'Call ID': row['Radio ID'],
                'Receive Tone': 'No'})
        except TypeError:
            pass
