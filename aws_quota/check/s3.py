from .quota_check import QuotaCheck, QuotaScope


class BucketCountCheck(QuotaCheck):
    key = "s3_bucket_count"
    description = "S3 Buckets per region"
    scope = QuotaScope.REGION
    service_code = 's3'
    quota_code = 'L-DC2B2D3D'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_buckets()['Buckets'])
