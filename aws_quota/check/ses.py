import boto3
from .quota_check import QuotaCheck, QuotaScope


class TopicCountCheck(QuotaCheck):
    key = "ses_daily_sends"
    description = "SES messages sent during the last 24 hours"
    scope = QuotaScope.REGION
    service_code = 'ses'
    quota_code = 'L-804C8AE8'
    used_services = [service_code]

    @property
    def current(self):
        return self.get_client(self.service_code).get_send_quota()['SentLast24Hours']
