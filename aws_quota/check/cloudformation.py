from .quota_check import QuotaCheck, QuotaScope


class StackCountCheck(QuotaCheck):
    key = "cf_stack_count"
    description = "Cloud Formation Stack count"
    scope = QuotaScope.ACCOUNT
    service_code = 'cloudformation'
    quota_code = 'L-0485CB21'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_stacks()['StackSummaries'])
