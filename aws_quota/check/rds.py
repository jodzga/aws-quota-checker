from aws_quota.utils import get_client
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope
import typing
from aws_quota import threadsafecache
import boto3


@threadsafecache.run_once_cache
def get_all_db_instances(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_db_instances')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['DBInstances']))


@threadsafecache.run_once_cache
def get_all_db_security_groups(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_db_security_groups')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['DBSecurityGroups']))


class RDSDBInstanceCountCheck(QuotaCheck):
    key = "rds_instances"
    description = "RDS instances per region"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-7B6409FD"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return len(get_all_db_instances(self.get_client(self.service_code)))


class RDSDBParameterGroupsCountCheck(QuotaCheck):
    key = "rds_parameter_groups"
    description = "RDS parameter groups per region"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-DE55804A"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return self.count_paginated_results("rds", "describe_db_parameter_groups", "DBParameterGroups")


class RDSDBClusterParameterGroupCountCheck(QuotaCheck):
    key = "rds_cluster_parameter_groups"
    description = "RDS cluster parameter groups per region"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-E4C808A8"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return self.count_paginated_results("rds", "describe_db_cluster_parameter_groups", "DBClusterParameterGroups")


class RDSEventSubscriptions(QuotaCheck):
    key = "rds_event_subscriptions"
    description = "RDS event subscriptions per region"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-A59F4C87"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return self.count_paginated_results("rds", "describe_event_subscriptions", "EventSubscriptionsList")


class RDSReadReplicasPerMasterCheck(InstanceQuotaCheck):
    key = "rds_read_replicas_per_master"
    description = "Read replicas per master"
    service_code = "rds"
    scope = QuotaScope.INSTANCE
    instance_id = 'DB Instance ID'
    quota_code = "L-5BC124EF"
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [db['DBInstanceIdentifier'] for db in get_all_db_instances(get_client(session, 'rds'))]

    @property
    def current(self) -> int:
        return len(next(filter(lambda db: self.instance_id == db['DBInstanceIdentifier'], get_all_db_instances(self.get_client(self.service_code))))['ReadReplicaDBInstanceIdentifiers'])


class RDSSecurityGroupsCheck(QuotaCheck):
    key = "rds_security_groups"
    description = "Security groups"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-732153D0"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return len(get_all_db_security_groups(self.get_client(self.service_code)))


class RDSSecurityGroupsVPCCheck(QuotaCheck):
    key = "rds_security_groups_vpc"
    description = "Security groups (VPC)"
    service_code = "rds"
    scope = QuotaScope.REGION
    quota_code = "L-36B04611"
    used_services = [service_code]

    @property
    def current(self) -> int:
        return len(list(filter(lambda sg: 'VpcId' in sg and sg['VpcId'] is not None, get_all_db_security_groups(self.get_client(self.service_code)))))


class RDSAuthorizationsPerDBSecurityGroupCheck(InstanceQuotaCheck):
    key = "rds_authorizations_per_db_security_group"
    description = "Authorizations per DB security group"
    service_code = "rds"
    scope = QuotaScope.INSTANCE
    instance_id = 'DB Security Group Name'
    quota_code = "L-AA8B1026"
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [db['DBSecurityGroupName'] for db in get_all_db_security_groups(get_client(session, 'rds'))]

    @property
    def current(self) -> int:
        sg = next(filter(lambda db: self.instance_id == db['DBSecurityGroupName'], get_all_db_security_groups(self.get_client(self.service_code))))
        return len(sg["EC2SecurityGroups"]) + len(sg["IPRanges"])
