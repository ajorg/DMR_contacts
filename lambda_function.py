#!/usr/bin/env python2
from StringIO import StringIO

import boto3

from dmr_marc_users_cs750 import (
    get_users, get_groups,
    write_contacts_csv,
    write_contacts_xlsx
    )


def s3_contacts(contacts, bucket, key):
    s3 = boto3.client('s3')

    o = StringIO()

    if key.endswith('.csv'):
        t = 'text/csv'
        write_contacts_csv(contacts, o)
    elif key.endswith('.xlsx'):
        t = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        write_contacts_xlsx(contacts, o)

    s3.put_object(
        Bucket=bucket, Key=key,
        Body=o.getvalue(), ContentType=t, ACL='public-read')
    o.close()


def lambda_handler(event=None, context=None):
    users = get_users()
    groups = get_groups()

    s3_contacts(contacts=users, bucket='dmr-contacts', key='DMR_contacts.csv')

    s3_contacts(contacts=groups+users,
                bucket='dmr-contacts', key='contacts-dci.xlsx')


if __name__ == '__main__':
    lambda_handler()
