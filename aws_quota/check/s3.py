from .quota_check import QuotaCheck, QuotaScope
from concurrent.futures import ThreadPoolExecutor


class BucketCountCheck(QuotaCheck):
    key = "s3_bucket_count"
    description = "S3 Buckets per region"
    scope = QuotaScope.REGION
    service_code = 's3'
    quota_code = 'L-DC2B2D3D'
    used_services = [service_code]

    def fetch_bucket_location(self, bucket):
        return self.get_client(self.service_code).get_bucket_location(Bucket=bucket['Name'])['LocationConstraint']

    @property
    def current(self):
        count = 0
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(self.fetch_bucket_location, bucket) for bucket in self.get_client(self.service_code).list_buckets()['Buckets']]
            for future in futures:
                location = future.result(60*5)
                if (location is None and self.boto_session.region_name == 'us-east-1') or location == self.boto_session.region_name:
                    count += 1
        return count
