#!/usr/bin/env python2
import csv
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# dmrx.net uses SSL SNI, which urllib2 doesn't support
import requests

from dmr_marc_users_cs750 import (get_groups_dci, get_groups_bm)

MOST_HEARD_URL = 'https://dmrx.net/csvfiles/MostHeard.csv'

# Neither of these formats uses a header row
COLUMNS_N0GSG = ('Call ID', 'Call Alias', 'Call Type', 'Receive Tone')
COLUMNS_DMRX = ('id', 'callsign', 'name')


def read_most_heard_csv(users):
    """Reads DMRX csv from the heard file-like object and returns a list of
    dicts in N0GSG export format."""
    csvr = csv.DictReader(users, fieldnames=COLUMNS_DMRX)
    result = []
    for row in csvr:
        result.append(dict(zip(COLUMNS_N0GSG, (
            row['id'],
            ' '.join((row['callsign'], row['name'])),
            'Private Call',  # Call Type
            'No',  # Receive Tone
            ))))
    return result


def write_n0gsg_csv(contacts, csvo,
                    fieldnames=COLUMNS_N0GSG, writeheader=False):
    """Writes contacts to the csvo file-like object.
    """
    csvw = csv.DictWriter(csvo, fieldnames)
    if writeheader:
        csvw.writeheader()
    for row in contacts:
        csvw.writerow(row)


def get_users(db_url=MOST_HEARD_URL):
    source = requests.get(db_url)
    data = source.content.decode('utf-8', 'replace').encode('ascii', 'replace')
    users = read_most_heard_csv(StringIO(str(data)))
    source.close()
    return users


if __name__ == '__main__':
    marc = get_users()
    dci = get_groups_dci()
    bm = get_groups_bm()

    with open('n0gsg-dci-bm-dmrx-most-heard.csv', 'w') as csvo:
        write_n0gsg_csv(dci + bm + marc, csvo)
