import boto3
import typing
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope
from aws_quota import threadsafecache
from aws_quota.utils import get_client
from concurrent.futures import ThreadPoolExecutor


def fetch_bucket_location(client, bucket):
    return (bucket['Name'], client.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint'])


@threadsafecache.run_once_cache
def get_all_buckets(client):
    return client.list_buckets()['Buckets']


@threadsafecache.run_once_cache
def get_buckets_for_region(client, region):
    result = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(fetch_bucket_location, client, bucket) for bucket in get_all_buckets(client)]
    for future in futures:
        (name, location) = future.result(60*5)
        if location == region or (location is None and region == 'us-east-1'):
            result.append(name)
    return result


class BucketCountCheck(QuotaCheck):
    key = "s3_bucket_count"
    description = "S3 Buckets per region"
    scope = QuotaScope.REGION
    service_code = 's3'
    quota_code = 'L-DC2B2D3D'
    used_services = [service_code]

    @property
    def current(self):
        return len(get_buckets_for_region(self.get_client(self.service_code), self.boto_session.region_name))


class BucketPolicyCheck(InstanceQuotaCheck):
    key = "s3_bucket_policy"
    description = "S3 Buckets policy"
    scope = QuotaScope.INSTANCE
    service_code = 's3'
    quota_code = 'L-748707F3'
    instance_id = 'Bucket Name'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return get_buckets_for_region(get_client(session, "s3"), session.region_name)

    @property
    def current(self):
        try:
            policy = self.get_client(self.service_code).get_bucket_policy(Bucket=self.instance_id)['Policy']
            # Assume 1 char = 1 byte
            return len(policy) / 1000.0
        except Exception as e:
            # Don't throw exception if bucket policy does not exist.
            if "The bucket policy does not exist" in str(e):
                return 0
            else:
                raise e


class BucketTagsCheck(InstanceQuotaCheck):
    key = "s3_bucket_tags"
    description = "S3 Buckets tags"
    scope = QuotaScope.INSTANCE
    service_code = 's3'
    quota_code = 'L-55BA2C6C'
    instance_id = 'Bucket Name'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return get_buckets_for_region(get_client(session, "s3"), session.region_name)

    @property
    def current(self):
        return len(self.get_client(self.service_code).get_bucket_tagging(Bucket=self.instance_id)['TagSet'])


class BucketLifecyleRulesCheck(InstanceQuotaCheck):
    key = "s3_bucket_lifecyle_rules"
    description = "S3 Buckets Lifecyle rules"
    scope = QuotaScope.INSTANCE
    service_code = 's3'
    quota_code = 'L-146D5F0C'
    instance_id = 'Bucket Name'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return get_buckets_for_region(get_client(session, "s3"), session.region_name)

    @property
    def current(self):
        try:
            return len(self.get_client(self.service_code).get_bucket_lifecycle_configuration(Bucket=self.instance_id)['Rules'])
        except Exception as e:
            # Don't throw exception if bucket lifecycle config does not exist.
            if "The lifecycle configuration does not exist" in str(e):
                return 0
            else:
                raise e
