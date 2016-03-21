#!/usr/bin/env python2
from StringIO import StringIO

import boto3

from dmr_marc_users_cs750 import (
    get_users, get_groups,
    write_contacts_csv,
    write_contacts_xlsx,
    )
from dmrx_most_heard_n0gsg import (
    get_users as get_most_heard,
    write_n0gsg_csv,
    )


def s3_contacts(contacts, bucket, key):
    s3 = boto3.client('s3')

    o = StringIO()

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
        Bucket=bucket, Key=key,
        Body=o.getvalue(), ContentType=t, ACL='public-read')
    o.close()


def lambda_handler(event=None, context=None):
    marc = get_users()
    dmrx = get_most_heard()
    groups = get_groups()

    s3_contacts(contacts=marc, bucket='dmr-contacts',
                key='CS750/DMR_contacts.csv')
    s3_contacts(contacts=groups+marc, bucket='dmr-contacts',
                key='CS750/dci-bm-marc.xlsx')
    s3_contacts(contacts=dmrx, bucket='dmr-contacts',
                key='N0GSG/dmrx-most-heard.csv')


if __name__ == '__main__':
    lambda_handler()
