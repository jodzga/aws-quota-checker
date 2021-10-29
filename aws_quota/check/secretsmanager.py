from .quota_check import QuotaCheck, QuotaScope


class SecretCountCheck(QuotaCheck):
    key = "secretsmanager_secrets_count"
    description = "Secrets per account"
    scope = QuotaScope.ACCOUNT
    service_code = 'secretsmanager'
    quota_code = 'L-2F66C23C'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_secrets()['SecretList'])
