import typing
import boto3
from aws_quota.utils import get_client
from aws_quota import threadsafecache
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope


@threadsafecache.run_once_cache
def get_all_asgs(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_auto_scaling_groups')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['AutoScalingGroups']))


@threadsafecache.run_once_cache
def get_all_asg_policies(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_policies')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['ScalingPolicies']))


class AutoScalingGroupCountCheck(QuotaCheck):
    key = "asg_count"
    description = "Auto Scaling groups per region"
    scope = QuotaScope.REGION
    service_code = 'autoscaling'
    quota_code = 'L-CDE20ADC'
    used_services = [service_code]

    @property
    def current(self):
        return len(get_all_asgs(self.get_client(self.service_code)))


class LaunchConfigurationCountCheck(QuotaCheck):
    key = "lc_count"
    description = "Launch configurations per region"
    scope = QuotaScope.REGION
    service_code = 'autoscaling'
    quota_code = 'L-6B80B8FA'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_launch_configurations()['LaunchConfigurations'])

# Checking classic LBs per ASG.
class ClassicLoadBalancersPerAutoScalingGroup(InstanceQuotaCheck):
    key = "asg_classic_lb_per_asg"
    description = "Classic Load Balancers per Auto Scaling group"
    scope = QuotaScope.INSTANCE
    service_code = 'autoscaling'
    quota_code = 'L-F786B2E5'
    instance_id = 'Auto Scaling Group ARN'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [asg['AutoScalingGroupARN'] for asg in get_all_asgs(get_client(session, 'autoscaling'))]

    @property
    def current(self) -> int:
        asgs = get_all_asgs(self.get_client(self.service_code))
        return len(next(filter(lambda asg: self.instance_id == asg['AutoScalingGroupARN'], asgs))['LoadBalancerNames'])


# Checking ALBs/NLBs per ASG.
class TargetGroupsPerAutoScalingGroup(InstanceQuotaCheck):
    key = "asg_target_groups_per_asg"
    description = "Target groups per Auto Scaling group"
    scope = QuotaScope.INSTANCE
    service_code = 'autoscaling'
    quota_code = 'L-05CB8B12'
    instance_id = 'Auto Scaling Group ARN'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [asg['AutoScalingGroupARN'] for asg in get_all_asgs(get_client(session, 'autoscaling'))]

    @property
    def current(self) -> int:
        asgs = get_all_asgs(self.get_client(self.service_code))
        return len(next(filter(lambda asg: self.instance_id == asg['AutoScalingGroupARN'], asgs))['TargetGroupARNs'])


class ScalingPoliciesPerAutoScalingGroup(InstanceQuotaCheck):
    key = "asg_scaling_policies_per_asg"
    description = "Scaling policies per Auto Scaling group"
    scope = QuotaScope.INSTANCE
    service_code = 'autoscaling'
    quota_code = 'L-72753F6F'
    instance_id = 'Auto Scaling Group Name'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [asg['AutoScalingGroupName'] for asg in get_all_asgs(get_client(session, 'autoscaling'))]

    @property
    def current(self) -> int:
        policies = get_all_asg_policies(self.get_client(self.service_code))
        return len(list(filter(lambda policy: self.instance_id == policy['AutoScalingGroupName'], policies)))
