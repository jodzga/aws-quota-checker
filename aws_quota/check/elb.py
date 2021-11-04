from aws_quota.utils import get_client
from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing
import boto3
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope
from aws_quota import threadsafecache



@threadsafecache.run_once_cache
def lbs(client, lb_type):
    paginator = client.get_paginator('describe_load_balancers')
    return list(
        filter(
            lambda lb: lb['Type'] == lb_type,
            (chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['LoadBalancers'])
        )
    )


def get_albs(client):
    return lbs(client, 'application')


def get_nlbs(client):
    return lbs(client, 'network')


@threadsafecache.run_once_cache
def get_target_groups(client, lb_arn):
    paginator = client.get_paginator('describe_target_groups')
    return list((chunk for page in paginator.paginate(LoadBalancerArn=lb_arn, PaginationConfig={'PageSize': 100}) for chunk in page['TargetGroups']))


@threadsafecache.run_once_cache
def get_listeners(client, lb_arn):
    paginator = client.get_paginator('describe_listeners')
    return list((chunk for page in paginator.paginate(LoadBalancerArn=lb_arn, PaginationConfig={'PageSize': 100}) for chunk in page['Listeners']))


@threadsafecache.run_once_cache
def get_rules(client, listener_arn):
    paginator = client.get_paginator('describe_rules')
    return list((chunk for page in paginator.paginate(ListenerArn=listener_arn, PaginationConfig={'PageSize': 100}) for chunk in page['Rules']))


@threadsafecache.run_once_cache
def get_target_health(client, tg_arn):
    return client.describe_target_health(TargetGroupArn=tg_arn)['TargetHealthDescriptions']


def get_target_count_per_target_group(client, tg_arn):
    return len(get_target_health(client, tg_arn))


@threadsafecache.run_once_cache
def get_all_target_groups(client):
    paginator = client.get_paginator('describe_target_groups')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['TargetGroups']))


class ClassicLoadBalancerCountCheck(QuotaCheck):
    key = "elb_clb_count"
    description = "Classic Load Balancers per region"
    scope = QuotaScope.REGION
    service_code = 'elasticloadbalancing'
    quota_code = 'L-E9E9831D'
    used_services = ['elb']

    @property
    def current(self):
        return len(
            self.get_client('elb').describe_load_balancers()['LoadBalancerDescriptions']
        )


class ListenerPerClassicLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_listeners_per_clb"
    description = "Listeners per Classic Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-1A491844'
    instance_id = 'Load Balancer Name'
    used_services = ['elb']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [
            lb['LoadBalancerName']
            for lb in get_client(session, 'elb').describe_load_balancers()['LoadBalancerDescriptions']
        ]

    @property
    def current(self):
        try:
            return len(
                self.get_client('elb').describe_load_balancers(
                    LoadBalancerNames=[self.instance_id]
                )['LoadBalancerDescriptions'][0]['ListenerDescriptions']
            )
        except self.get_client('elb').exceptions.AccessPointNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class NetworkLoadBalancerCountCheck(QuotaCheck):
    key = "elb_nlb_count"
    description = "Network Load Balancers per region"
    scope = QuotaScope.REGION
    service_code = 'elasticloadbalancing'
    quota_code = 'L-69A177A2'
    used_services = ['elbv2']

    @property
    def current(self):
        return len(
            list(
                filter(
                    lambda lb: lb['Type'] == 'network',
                        self.get_client('elbv2').describe_load_balancers()['LoadBalancers'],
                )
            )
        )


class ListenerPerNetworkLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_listeners_per_nlb"
    description = "Listeners per Network Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-57A373D6'
    instance_id = 'Load Balancer ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [alb['LoadBalancerArn'] for alb in get_nlbs(get_client(session, 'elbv2'))]

    @property
    def current(self):
        try:
            return len(
                self.get_client('elbv2').describe_listeners(
                    LoadBalancerArn=self.instance_id
                )['Listeners']
            )
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class TargetsPerNetworkLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_targets_per_nlb"
    description = "Targets per Network Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-EEF1AD04'
    instance_id = 'Load Balancer ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [alb['LoadBalancerArn'] for alb in get_nlbs(get_client(session, 'elbv2'))]

    @property
    def current(self):
        try:
            count = 0
            for target_group in get_target_groups(self.get_client('elbv2'), self.instance_id):
                count += get_target_count_per_target_group(self.get_client('elbv2'), target_group['TargetGroupArn'])
            return count
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


# The Availability Zone in this check can be interpreted in two ways:
# a) When we register a Target with load balancer we provide an Availability Zone which determines whether the target
#    receives traffic from the load balancer nodes in the specified Availability Zone or from all enabled Availability Zones
#    for the load balancer (see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/elbv2.html#ElasticLoadBalancingv2.Client.register_targets).
# b) An Availability Zone where the target is located.
# We implement a) here and wait for clarification from AWS.
class TargetsPerAZPerNetworkLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_targets_per_az_per_nlb"
    description = "Targets per Availability Zone per Network Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-B211E961'
    instance_id = 'Load Balancer ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [alb['LoadBalancerArn'] for alb in get_nlbs(get_client(session, 'elbv2'))]

    @property
    def current(self):
        try:
            counts = {}
            all = 0
            for target_group in get_target_groups(self.get_client('elbv2'), self.instance_id):
                for tg_health in get_target_health(self.get_client('elbv2'), target_group['TargetGroupArn']):
                    tg = tg_health['Target']
                    if 'AvailabilityZone' not in tg or tg['AvailabilityZone'] == 'all':
                        all += 1
                    else:
                        if tg['AvailabilityZone'] not in counts:
                            counts[tg['AvailabilityZone']] = 0
                        tg['AvailabilityZone'] += 1
            if not counts:
                return all
            else:
                for az in counts:
                    counts[az] += all
                return max(counts.values)
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class TargetsPerTargetGroupPerRegionCountCheck(InstanceQuotaCheck):
    key = "elb_targets_per_target_group_per_region"
    description = "Targets per Target Group per Region"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-A0D0B863'
    instance_id = 'Target Group ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [tg['TargetGroupArn'] for tg in get_all_target_groups(get_client(session, 'elbv2'))]

    @property
    def current(self):
        try:
            return get_target_count_per_target_group(self.get_client('elbv2'), self.instance_id)
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class ApplicationLoadBalancerCountCheck(QuotaCheck):
    key = "elb_alb_count"
    description = "Application Load Balancers per region"
    scope = QuotaScope.REGION
    service_code = 'elasticloadbalancing'
    quota_code = 'L-53DA6B97'
    used_services = ['elbv2']

    @property
    def current(self):
        return len(get_albs(self.get_client('elbv2')))


class ListenerPerApplicationLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_listeners_per_alb"
    description = "Listeners per Application Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-B6DF7632'
    instance_id = 'Load Balancer ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [alb['LoadBalancerArn'] for alb in get_albs(get_client(session, 'elbv2'))]

    @property
    def current(self) -> int:
        try:
            return len(
                self.get_client('elbv2').describe_listeners(
                    LoadBalancerArn=self.instance_id
                )['Listeners']
            )
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class TargetGroupCountCheck(QuotaCheck):
    key = "elb_target_group_count"
    description = "Target Groups per region"
    scope = QuotaScope.REGION
    service_code = 'elasticloadbalancing'
    quota_code = 'L-B22855CB'
    used_services = ['elbv2']

    @property
    def current(self):
        return len(get_all_target_groups(self.get_client('elbv2')))


class TargetGroupsPerApplicationLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_target_groups_per_alb"
    description = "Target groups per Application Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-822D1B1B'
    instance_id = 'Load Balancer ARN'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [alb['LoadBalancerArn'] for alb in get_albs(get_client(session, 'elbv2'))]

    @property
    def current(self) -> int:
        try:
            return len(get_target_groups(self.get_client('elbv2'), self.instance_id))
        except self.get_client('elbv2').exceptions.LoadBalancerNotFoundException as e:
            raise InstanceWithIdentifierNotFound(self) from e


class TargetGroupsPerActionPerNetworkLoadBalancerCountCheck(InstanceQuotaCheck):
    key = "elb_target_groups_per_action_per_nlb"
    description = "Target Groups per Action per Network Load Balancer"
    service_code = 'elasticloadbalancing'
    quota_code = 'L-AFDDADBF'
    instance_id = '(Load Balancer ARN, Listener ARN, Rule ARN)'
    used_services = ['elbv2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        indentifiers = []
        for nlb in get_nlbs(get_client(session, 'elbv2')):
            for listener in get_listeners(get_client(session, 'elbv2'), nlb['LoadBalancerArn']):
                indentifiers.append((nlb['LoadBalancerArn'], listener['ListenerArn'], None))
                indentifiers += [(nlb['LoadBalancerArn'], listener['ListenerArn'], rule['RuleArn']) for rule in get_rules(get_client(session, 'elbv2'), listener['ListenerArn'])]
        return indentifiers

    @property
    def current(self) -> int:
        count = 0
        nlb_anr, listener_anr, rule_anr = self.instance_id
        if rule_anr is None:
            listener = next((x for x in get_listeners(self.get_client('elbv2'), nlb_anr) if x['ListenerArn'] == listener_anr), None)
            if listener is None:
                raise InstanceWithIdentifierNotFound(self)
            for action in listener['DefaultActions']:
                processed_target_groups = set()
                if 'TargetGroupArn' in action and action['TargetGroupArn'] is not None:
                    count += 1
                    processed_target_groups.add(action['TargetGroupArn'])
                if 'ForwardConfig' in action:
                    for target_group in action['ForwardConfig']['TargetGroups']:
                        if target_group['TargetGroupArn'] not in processed_target_groups:
                            count += 1
                            processed_target_groups.add(target_group['TargetGroupArn'])
        else:
            rule = next((x for x in get_rules(self.get_client('elbv2'), listener_anr) if x['RuleArn'] == rule_anr), None)
            if rule is None:
                raise InstanceWithIdentifierNotFound(self)
            for action in rule['Actions']:
                processed_target_groups = set()
                if 'TargetGroupArn' in action and action['TargetGroupArn'] is not None:
                    count += 1
                    processed_target_groups.add(action['TargetGroupArn'])
                if 'ForwardConfig' in action:
                    for target_group in action['ForwardConfig']['TargetGroups']:
                        if target_group['TargetGroupArn'] not in processed_target_groups:
                            count += 1
                            processed_target_groups.add(target_group['TargetGroupArn'])
        return count
