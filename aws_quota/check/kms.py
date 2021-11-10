from aws_quota.utils import get_client
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope
import typing
from aws_quota import threadsafecache
import boto3


@threadsafecache.run_once_cache
def get_all_keys(client) -> typing.List[dict]:
    paginator = client.get_paginator('list_keys')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['Keys']))


@threadsafecache.run_once_cache
def get_all_aliases(client) -> typing.List[dict]:
    paginator = client.get_paginator('list_aliases')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['Aliases']))


class KMSKeysCountCheck(QuotaCheck):
    key = "kms_keys"
    description = "Customer Master Keys (CMKs)"
    service_code = "kms"
    scope = QuotaScope.REGION
    quota_code = "L-C2F1777E"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return len(get_all_keys(self.get_client(self.service_code)))


class KMSAliasesPerCMKCheck(InstanceQuotaCheck):
    key = "kms_aliases_per_cmk"
    description = "Aliases per CMK"
    service_code = "kms"
    scope = QuotaScope.INSTANCE
    instance_id = 'Key ID'
    quota_code = "L-340F62FB"
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [key['KeyId'] for key in get_all_keys(get_client(session, 'kms'))]

    @property
    def current(self) -> int:
        return len(list(filter(lambda alias: 'TargetKeyId' in alias and self.instance_id == alias['TargetKeyId'], get_all_aliases(self.get_client(self.service_code)))))
