from aws_quota.utils import get_client
from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing

import boto3
import botocore.exceptions
from aws_quota import threadsafecache
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope


@threadsafecache.run_once_cache
def get_all_eks_clusters(client) -> typing.List[dict]:
    paginator = client.get_paginator('list_clusters')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['clusters']))


class ClusterCountCheck(QuotaCheck):
    key = "eks_count"
    description = "EKS Clusters per region"
    scope = QuotaScope.REGION
    service_code = 'eks'
    quota_code = 'L-1194D53C'
    used_services = [service_code]

    @property
    def current(self):
        return len(get_all_eks_clusters(self.get_client(self.service_code)))


class ControlPlaneSecurityGroupsPerCluster(InstanceQuotaCheck):
    key = "eks_control_plane_sg_per_cluster"
    description = "Control plane security groups per cluster"
    scope = QuotaScope.INSTANCE
    service_code = 'eks'
    quota_code = 'L-11427A54'
    instance_id = 'Cluster'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return get_all_eks_clusters(get_client(session, 'eks'))

    @property
    def current(self):
        eks = self.get_client(self.service_code).describe_cluster(name=self.instance_id)
        # securityGroupIds:
        # The security groups associated with the cross-account elastic network interfaces that are used to allow
        # communication between your nodes and the Kubernetes control plane.
        result = len(eks['cluster']['resourcesVpcConfig']['securityGroupIds'])

        # clusterSecurityGroupId:
        # The cluster security group that was created by Amazon EKS for the cluster. Managed node groups use this
        # security group for control-plane-to-data-plane communication.
        if 'clusterSecurityGroupId' in eks['cluster']['resourcesVpcConfig']\
            and eks['cluster']['resourcesVpcConfig']['clusterSecurityGroupId']:
            result += 1
        return result
