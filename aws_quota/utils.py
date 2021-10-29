import functools
import boto3


@functools.cache
def get_account_id(session: boto3.Session) -> str:
    return get_client(session, 'sts').get_caller_identity()['Account']

@functools.cache
def get_client(session: boto3.Session, service_name) -> str:
    return session.client(service_name)

