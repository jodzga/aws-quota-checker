from .quota_check import QuotaCheck, QuotaScope


class TableCountCheck(QuotaCheck):
    key = "dyndb_table_count"
    description = "DynamoDB Tables per region"
    scope = QuotaScope.REGION
    service_code = 'dynamodb'
    quota_code = 'L-F98FE922'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_tables()['TableNames'])
