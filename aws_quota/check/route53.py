from aws_quota.utils import get_client
from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing
import boto3
from .quota_check import InstanceQuotaCheck, QuotaCheck, QuotaScope


class HostedZoneCountCheck(QuotaCheck):
    key = "route53_hosted_zone_count"
    description = "Route53 Hosted Zones per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'route53'
    quota_code = 'L-4EA4796A'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_HOSTED_ZONES_BY_OWNER')['Limit']['Value']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_HOSTED_ZONES_BY_OWNER')['Count']


class HealthCheckCountCheck(QuotaCheck):
    key = "route53_health_check_count"
    description = "Route53 Health Checks per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'route53'
    quota_code = 'L-ACB674F3'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_HEALTH_CHECKS_BY_OWNER')['Limit']['Value']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_HEALTH_CHECKS_BY_OWNER')['Count']


class ReusableDelegationSetCountCheck(QuotaCheck):
    key = "route53_reusable_delegation_set_count"
    description = "Route53 Reusable Delegation Sets per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'route53'
    quota_code = 'L-A72C7724'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_REUSABLE_DELEGATION_SETS_BY_OWNER')['Limit']['Value']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_REUSABLE_DELEGATION_SETS_BY_OWNER')['Count']


class TrafficPolicyCountCheck(QuotaCheck):
    key = "route53_traffic_policy_count"
    description = "Route53 Traffic Policies per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'route53'
    quota_code = 'L-FC688E7C'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_TRAFFIC_POLICIES_BY_OWNER')['Limit']['Value']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_TRAFFIC_POLICIES_BY_OWNER')['Count']


class TrafficPolicyInstanceCountCheck(QuotaCheck):
    key = "route53_traffic_policy_instance_count"
    description = "Route53 Traffic Policy Instances per Account"
    scope = QuotaScope.ACCOUNT
    service_code = 'route53'
    quota_code = 'L-628D5A56'
    used_services = [service_code]

    @property
    def maximum(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_TRAFFIC_POLICY_INSTANCES_BY_OWNER')['Limit']['Value']

    @property
    def current(self):
        return self.get_client(self.service_code).get_account_limit(Type='MAX_TRAFFIC_POLICY_INSTANCES_BY_OWNER')['Count']


class RecordsPerHostedZoneCheck(InstanceQuotaCheck):
    key = "route53_records_per_hosted_zone"
    description = "Records per Route53 Hosted Zone"
    scope = QuotaScope.INSTANCE
    service_code = 'route53'
    instance_id = 'Hosted Zone ID'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [zone['Id'] for zone in get_client(session, 'route53').list_hosted_zones()['HostedZones']]

    @property
    def maximum(self):
        try:
            return self.get_client(self.service_code).get_hosted_zone_limit(Type='MAX_RRSETS_BY_ZONE', HostedZoneId=self.instance_id)['Limit']['Value']
        except self.get_client(self.service_code).exceptions.NoSuchHostedZone as e:
            raise InstanceWithIdentifierNotFound(self) from e

    @property
    def current(self):
        try:
            return self.get_client(self.service_code).get_hosted_zone_limit(Type='MAX_RRSETS_BY_ZONE', HostedZoneId=self.instance_id)['Count']
        except self.get_client(self.service_code).exceptions.NoSuchHostedZone as e:
            raise InstanceWithIdentifierNotFound(self) from e


class AssociatedVpcHostedZoneCheck(InstanceQuotaCheck):
    key = "route53_vpcs_per_hosted_zone"
    description = "Associated VPCs per Route53 Hosted Zone"
    scope = QuotaScope.INSTANCE
    instance_id = 'Hosted Zone ID'
    service_code = 'route53'
    quota_code = 'L-84B40094'

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [zone['Id'] for zone in get_client(session, 'route53').list_hosted_zones()['HostedZones'] if zone['Config']['PrivateZone']]

    @property
    def maximum(self):
        try:
            return self.get_client(self.service_code).get_hosted_zone_limit(Type='MAX_VPCS_ASSOCIATED_BY_ZONE', HostedZoneId=self.instance_id)['Limit']['Value']
        except self.get_client(self.service_code).exceptions.NoSuchHostedZone as e:
            raise InstanceWithIdentifierNotFound(self) from e

    @property
    def current(self):
        try:
            return self.get_client(self.service_code).get_hosted_zone_limit(Type='MAX_VPCS_ASSOCIATED_BY_ZONE', HostedZoneId=self.instance_id)['Count']
        except self.get_client(self.service_code).exceptions.NoSuchHostedZone as e:
            raise InstanceWithIdentifierNotFound(self) from e
