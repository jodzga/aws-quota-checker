from .quota_check import QuotaCheck, QuotaScope


class AutoScalingGroupCountCheck(QuotaCheck):
    key = "asg_count"
    description = "Auto Scaling groups per region"
    scope = QuotaScope.REGION
    service_code = 'autoscaling'
    quota_code = 'L-CDE20ADC'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_auto_scaling_groups()['AutoScalingGroups'])


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
