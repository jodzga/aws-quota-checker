from .quota_check import QuotaCheck, QuotaScope


class FunctionAndLayerStorageCheck(QuotaCheck):
    key = "lambda_function_storage"
    description = "Lambda function and layer storage"
    scope = QuotaScope.REGION
    service_code = 'lambda'
    quota_code = 'L-2ACBD22F'
    used_services = [service_code]

    @property
    def current(self):
        return (
            self.get_client(self.service_code).get_account_settings()['AccountUsage'][
                'TotalCodeSize'
            ]
            / 1000000000
        )
