from .quota_check import QuotaCheck, QuotaScope


class ApplicationCountCheck(QuotaCheck):
    key = "elasticbeanstalk_application_count"
    description = "Elastic Beanstalk Applications per account"
    scope = QuotaScope.ACCOUNT
    service_code = 'elasticbeanstalk'
    quota_code = 'L-1CEABD17'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_applications()['Applications'])


class EnvironmentCountCheck(QuotaCheck):
    key = "elasticbeanstalk_environment_count"
    description = "Elastic Beanstalk Environments per account"
    scope = QuotaScope.ACCOUNT
    service_code = 'elasticbeanstalk'
    quota_code = 'L-8EFC1C51'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_environments()['Environments'])
