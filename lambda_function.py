#!/usr/bin/env python2
import logging

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import boto3

from dmr_marc_users_cs750 import (
    get_users,
    get_groups_dci, get_groups_bm,
    write_contacts_csv,
    write_contacts_xlsx,
    SIMPLEX,
    )
from dmrx_most_heard_n0gsg import (
    get_users as get_most_heard,
    write_n0gsg_csv,
    )

logger = logging.getLogger(__name__)


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
        Bucket=bucket, Key=key, StorageClass='REDUCED_REDUNDANCY',
        Body=o.getvalue(), ContentType=t, ACL='public-read')
    o.close()


def lambda_handler(event=None, context=None):
    logger.info('Getting MARC Users')
    marc = get_users('s3://dmr-contacts/marc/users.csv')
    logger.info('Getting BrandMeister Groups')
    bm = get_groups_bm('s3://dmr-contacts/brandmeister/groups.json')
    logger.info('Getting DMRX Most Heard')
    dmrx = get_most_heard()
    logger.info('Getting DCI Groups')
    dci = get_groups_dci()

    logger.info('Writing CS750 Contacts .csv')
    s3_contacts(contacts=marc, bucket='dmr-contacts',
                key='CS750/DMR_contacts.csv')
    logger.info('Writing CS750 .xlsx')
    s3_contacts(contacts=SIMPLEX + dci + bm + marc, bucket='dmr-contacts',
                key='CS750/dci-bm-marc.xlsx')
    logger.info('Writing DMRX Most Heard (N0GSG) .csv')
    s3_contacts(contacts=dmrx, bucket='dmr-contacts',
                key='N0GSG/dmrx-most-heard.csv')
    logger.info('Writing BrandMeister Groups (N0GSG) .csv')
    s3_contacts(contacts=bm, bucket='dmr-contacts',
                key='N0GSG/brandmeister-groups.csv')


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(message)s')
    logger.setLevel(logging.INFO)
    lambda_handler()
