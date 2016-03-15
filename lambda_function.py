#!/usr/bin/env python2
from StringIO import StringIO

import boto3

from dmr_marc_users_cs750 import (
    get_users, get_groups,
    write_contacts_csv,
    write_contacts_xlsx
    )


def lambda_handler(event=None, context=None):
    users = get_users()

    csvo = StringIO()
    write_contacts_csv(users, csvo)

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='dmr-contacts', Key='DMR_contacts.csv',
        Body=csvo.getvalue(), ContentType='text/csv', ACL='public-read')

    csvo.close()

    groups = get_groups()

    xlsxo = StringIO()
    write_contacts_xlsx(groups + users, xlsxo)
    s3.put_object(
        Bucket='dmr-contacts', Key='contacts-dci.xlsx',
        Body=xlsxo.getvalue(),
        ContentType=('application/'
                     'vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        ACL='public-read')
    xlsxo.close()
