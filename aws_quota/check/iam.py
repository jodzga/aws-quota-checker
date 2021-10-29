from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing

import boto3
from .quota_check import InstanceQuotaCheck, QuotaCheck, QuotaScope


class GroupCountCheck(QuotaCheck):
    key = "iam_group_count"
    description = "IAM groups per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-F4A5425F'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['GroupsQuota']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['Groups']


class UsersCountCheck(QuotaCheck):
    key = "iam_user_count"
    description = "IAM users per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-F55AF5E4'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['UsersQuota']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['Users']


class PolicyCountCheck(QuotaCheck):
    key = "iam_policy_count"
    description = "IAM policies per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-E95E4862'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['PoliciesQuota']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['Policies']


class PolicyVersionCountCheck(QuotaCheck):
    key = "iam_policy_version_count"
    description = "IAM policy versions in use per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['PolicyVersionsInUseQuota']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['PolicyVersionsInUse']


class ServerCertificateCountCheck(QuotaCheck):
    key = "iam_server_certificate_count"
    description = "IAM server certificates per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'iam'
    quota_code = 'L-BF35879D'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['ServerCertificatesQuota']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['ServerCertificates']


class AttachedPolicyPerUserCheck(InstanceQuotaCheck):
    key = "iam_attached_policy_per_user"
    description = "Attached IAM policies per user"
    instance_id = "User Name"
    service_code = 'iam'
    quota_code = 'L-4019AD8B'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [user['UserName'] for user in session.client('iam').list_users()['Users']]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['AttachedPoliciesPerUserQuota']

    @property
    def current(self):
        try:
            return len(self.get_client(self.service_code).list_user_policies(UserName=self.instance_id)['PolicyNames'])
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
        return [user['GroupName'] for user in session.client('iam').list_groups()['Groups']]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['AttachedPoliciesPerGroupQuota']

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
        return [user['RoleName'] for user in session.client('iam').list_roles()['Roles']]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_summary()['SummaryMap']['AttachedPoliciesPerRoleQuota']

    @property
    def current(self):
        try:
            return len(self.get_client(self.service_code).list_role_policies(RoleName=self.instance_id)['PolicyNames'])
        except self.get_client(self.service_code).exceptions.NoSuchEntityException as e:
            raise InstanceWithIdentifierNotFound(self) from e
