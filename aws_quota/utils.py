from aws_quota import threadsafecache
import boto3
from botocore.config import Config
import click
from concurrent.futures import ThreadPoolExecutor


@threadsafecache.run_once_cache
def get_account_id(session: boto3.Session) -> str:
    return get_client(session, 'sts').get_caller_identity()['Account']

@threadsafecache.run_once_cache
def get_client(session: boto3.Session, service_name) -> str:
    return session.client(service_name, config=Config(retries={'max_attempts': 10, 'mode': 'adaptive'}))

quota_values = {}

def fetch_quotas(client, service, paginator_name):
    paginator = client.get_paginator(paginator_name)
    return list((quot for page in paginator.paginate(ServiceCode = service['ServiceCode'], PaginationConfig={'PageSize': 100}) for quot in page['Quotas']))

def initialize_quotas(session: boto3.Session):
    client = get_client(session, 'service-quotas')
    paginator = client.get_paginator('list_services')
    services = list((ser for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for ser in page['Services']))

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [(service, executor.submit(fetch_quotas, client, service, 'list_aws_default_service_quotas')) for service in services]
        with click.progressbar(futures, label='Fetching default quotas', show_pos=True) as futures:
            for (service, future) in futures:
                for quota in future.result(60*5):
                    quota_values[(service['ServiceCode'], quota['QuotaCode'])] = quota['Value']
        futures = [(service, executor.submit(fetch_quotas, client, service, 'list_service_quotas')) for service in services]
        with click.progressbar(futures, label='Fetching applied quotas', show_pos=True) as futures:
            for (service, future) in futures:
                for quota in future.result(60*5):
                    quota_values[(service['ServiceCode'], quota['QuotaCode'])] = quota['Value']


def get_quota(service_code, quota_code):
    return quota_values[(service_code, quota_code)]
