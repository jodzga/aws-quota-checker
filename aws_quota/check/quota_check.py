from aws_quota.utils import get_account_id, get_client as util_get_client
import enum
import typing

import boto3


class QuotaScope(enum.Enum):
    ACCOUNT = 0
    REGION = 1
    INSTANCE = 2


class QuotaCheck:
    key: str = None
    description: str = None
    scope: QuotaScope = None
    service_code: str = None
    quota_code: str = None
    used_services = []

    def __init__(self, boto_session: boto3.Session) -> None:
        super().__init__()

        self.boto_session = boto_session
        self.initialize_clients(['service-quotas'] + self.used_services)

    def get_client(self, service):
        return util_get_client(self.boto_session, service)
    
    def initialize_clients(self, used_services):
        for service in used_services:
            self.get_client(service)

    def __str__(self) -> str:
        return f'{self.key}{self.label_values}'
    
    def count_paginated_results(self, service: str, method: str, key: str, paginate_args: dict = {}) -> int:
        paginator = self.get_client(service).get_paginator(method)
        page_iterable = paginator.paginate(**paginate_args)
        return sum(len(page[key]) for page in page_iterable)

    @property
    def label_values(self):
        if self.scope == QuotaScope.ACCOUNT:
            return {'account': get_account_id(self.boto_session)}
        elif self.scope == QuotaScope.REGION:
            return {'account': get_account_id(self.boto_session), 'region': self.boto_session.region_name}
        elif self.scope == QuotaScope.INSTANCE:
            return {
                'account': get_account_id(self.boto_session),
                'region': self.boto_session.region_name,
                'instance': self.instance_id
            }

    @property
    def maximum(self) -> int:
        try:
            return int(self.get_client('service-quotas').get_service_quota(ServiceCode=self.service_code, QuotaCode=self.quota_code)['Quota']['Value'])
        except self.get_client('service-quotas').exceptions.NoSuchResourceException:
            return int(self.get_client('service-quotas').get_aws_default_service_quota(ServiceCode=self.service_code, QuotaCode=self.quota_code)['Quota']['Value'])

    @property
    def current(self) -> int:
        raise NotImplementedError


class InstanceQuotaCheck(QuotaCheck):
    scope = QuotaScope.INSTANCE
    instance_id: str = None

    def __init__(self, boto_session: boto3.Session, instance_id) -> None:
        super().__init__(boto_session)

        self.instance_id = instance_id

    @staticmethod
    def get_all_identifiers(boto_session: boto3.Session) -> typing.List[str]:
        raise NotImplementedError
