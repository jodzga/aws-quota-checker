from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing
import re
import boto3
from .quota_check import InstanceQuotaCheck, QuotaCheck, QuotaScope
from aws_quota import threadsafecache
from aws_quota.utils import get_client
import json


@threadsafecache.run_once_cache
def get_account_summary(client):
    return client.get_account_summary()['SummaryMap']


@threadsafecache.run_once_cache
def get_users(client):
    paginator = client.get_paginator('list_users')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['Users']))


class GroupCountCheck(QuotaCheck):
    key = "iam_group_count"
    description = "IAM groups per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-F4A5425F'
    used_services = [service_code]

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['GroupsQuota']

    @property
    def current(self):
        return get_account_summary(self.get_client(self.service_code))['Groups']


class UsersCountCheck(QuotaCheck):
    key = "iam_user_count"
    description = "IAM users per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-F55AF5E4'
    used_services = [service_code]

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['UsersQuota']

    @property
    def current(self):
        return get_account_summary(self.get_client(self.service_code))['Users']


class PolicyCountCheck(QuotaCheck):
    key = "iam_policy_count"
    description = "IAM policies per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-E95E4862'
    used_services = [service_code]

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['PoliciesQuota']

    @property
    def current(self):
        return get_account_summary(self.get_client(self.service_code))['Policies']


class PolicyVersionCountCheck(QuotaCheck):
    key = "iam_policy_version_count"
    description = "IAM policy versions in use per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    used_services = [service_code]

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['PolicyVersionsInUseQuota']

    @property
    def current(self):
        return get_account_summary(self.get_client(self.service_code))['PolicyVersionsInUse']


class ServerCertificateCountCheck(QuotaCheck):
    key = "iam_server_certificate_count"
    description = "IAM server certificates per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-BF35879D'
    used_services = [service_code]

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['ServerCertificatesQuota']

    @property
    def current(self):
        return get_account_summary(self.get_client(self.service_code))['ServerCertificates']


class AttachedPolicyPerUserCheck(InstanceQuotaCheck):
    key = "iam_attached_policy_per_user"
    description = "Attached IAM policies per user"
    instance_id = "User Name"
    service_code = 'iam'
    quota_code = 'L-4019AD8B'
    used_services = [service_code]
    concurrency = 32

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            return [user['UserName'] for user in get_users(get_client(session, 'iam'))]
        else:
            return []

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['AttachedPoliciesPerUserQuota']

    @property
    def current(self):
        try:
            paginator = self.get_client(self.service_code).get_paginator('list_user_policies')
            policy_names = list((chunk for page in paginator.paginate(UserName=self.instance_id, PaginationConfig={'PageSize': 100}) for chunk in page['PolicyNames']))
            return len(policy_names)
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class GroupsPerUserCheck(InstanceQuotaCheck):
    key = "iam_groups_per_user"
    description = "IAM groups per user"
    instance_id = "User Name"
    service_code = 'iam'
    quota_code = 'L-7A1621EC'
    used_services = [service_code]
    concurrency = 32

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            return [user['UserName'] for user in get_users(get_client(session, 'iam'))]
        else:
            return []

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['GroupsPerUserQuota']

    @property
    def current(self):
        try:
            paginator = self.get_client(self.service_code).get_paginator('list_groups_for_user')
            groups = list((chunk for page in paginator.paginate(UserName=self.instance_id, PaginationConfig={'PageSize': 100}) for chunk in page['Groups']))
            return len(groups)
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class ManagedPolicyLengthCheck(InstanceQuotaCheck):
    key = "iam_managed_policy_length"
    description = "Managed policy length"
    instance_id = "(Policy ARN, Version Id)"
    service_code = 'iam'
    quota_code = 'L-ED111B8C'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            paginator = get_client(session, 'iam').get_paginator('list_policies')
            policies = list((chunk for page in paginator.paginate(Scope='Local', PaginationConfig={'PageSize': 100}) for chunk in page['Policies']))
            return [(p['Arn'], p['DefaultVersionId']) for p in policies]
        else:
            return []

    @property
    def current(self):
        try:
            arn, version = self.instance_id
            policy = self.get_client(self.service_code).get_policy_version(PolicyArn=arn, VersionId=version)
            policy_json = json.dumps(policy['PolicyVersion']['Document'])
            # IAM does not count white space when calculating the size of a policy against this quota,
            # see https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html#reference_iam-quotas-entity-length
            return len(re.sub(r"\s", "", policy_json))
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class AccessKeysPerUserCheck(InstanceQuotaCheck):
    key = "iam_access_keys_per_user"
    description = "Access keys per user"
    instance_id = "User Name"
    service_code = 'iam'
    quota_code = 'L-8758042E'
    used_services = [service_code]
    concurrency = 32

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            return [user['UserName'] for user in get_users(get_client(session, 'iam'))]
        else:
            return []

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['AccessKeysPerUserQuota']

    @property
    def current(self):
        try:
            paginator = self.get_client(self.service_code).get_paginator('list_access_keys')
            access_keys_metadatas = list((chunk for page in paginator.paginate(UserName=self.instance_id, PaginationConfig={'PageSize': 100}) for chunk in page['AccessKeyMetadata']))
            return len(access_keys_metadatas)
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class AttachedPolicyPerGroupCheck(InstanceQuotaCheck):
    key = "iam_attached_policy_per_group"
    description = "Attached IAM policies per group"
    instance_id = "Group Name"
    service_code = 'iam'
    quota_code = 'L-384571C4'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            return [user['GroupName'] for user in session.client('iam').list_groups()['Groups']]
        else:
            return []

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['AttachedPoliciesPerGroupQuota']

    @property
    def current(self):
        try:
            return len(self.get_client(self.service_code).list_group_policies(GroupName=self.instance_id)['PolicyNames'])
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e

class AttachedPolicyPerRoleCheck(InstanceQuotaCheck):
    key = "iam_attached_policy_per_role"
    description = "Attached IAM policies per role"
    instance_id = "Role Name"
    service_code = 'iam'
    quota_code = 'L-0DA4ABF3'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        # The IAM is a global service. It can be accessed from any region and in any region the results are exactly the same.
        # However, only in us-east-1 the Service Quota returns the limit value for IAM. In all other regions the quota value
        # is missing for IAM. The us-east-1 is the "main" region in some sense.
        if session.region_name == 'us-east-1':
            return [user['RoleName'] for user in session.client('iam').list_roles()['Roles']]
        else:
            return []

    @property
    def maximum(self):
        return get_account_summary(self.get_client(self.service_code))['AttachedPoliciesPerRoleQuota']

    @property
    def current(self):
        try:
            return len(self.get_client(self.service_code).list_role_policies(RoleName=self.instance_id)['PolicyNames'])
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e
