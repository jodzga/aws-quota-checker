from aws_quota.utils import get_client
from aws_quota.exceptions import InstanceWithIdentifierNotFound
import typing

import boto3
import botocore.exceptions
from aws_quota import threadsafecache
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope


def check_if_vpc_exists(client, vpc_id: str) -> bool:
    try:
        next(filter(lambda vpc: vpc_id == vpc['VpcId'], get_all_vpcs(client)))
        return True
    except StopIteration:
        return False


@threadsafecache.run_once_cache
def get_all_vpcs(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_vpcs')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['Vpcs']))


def get_vpc_by_id(client, vpc_id: str) -> dict:
    try:
        return next(filter(lambda vpc: vpc_id == vpc['VpcId'], get_all_vpcs(client)))
    except StopIteration:
        raise KeyError


@threadsafecache.run_once_cache
def get_all_sgs(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_security_groups')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['SecurityGroups']))


def get_sg_by_id(client, sg_id: str) -> dict:
    try:
        return next(filter(lambda sg: sg_id == sg['GroupId'], get_all_sgs(client)))
    except StopIteration:
        raise KeyError


@threadsafecache.run_once_cache
def get_all_rts(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_route_tables')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['RouteTables']))


def get_rt_by_id(client, rt_id: str) -> dict:
    try:
        return next(filter(lambda rt: rt_id == rt['RouteTableId'], get_all_rts(client)))
    except StopIteration:
        raise KeyError


@threadsafecache.run_once_cache
def get_all_network_acls(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_network_acls')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['NetworkAcls']))


def get_subnet_by_arn(client, subnet_arn: str) -> dict:
    try:
        return next(filter(lambda subnet: subnet_arn == subnet['SubnetArn'], get_all_subnets(client)))
    except StopIteration:
        raise KeyError


@threadsafecache.run_once_cache
def get_all_subnets(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_subnets')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['Subnets']))


@threadsafecache.run_once_cache
def get_vpc_peering_connections(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_vpc_peering_connections')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['VpcPeeringConnections']))


@threadsafecache.run_once_cache
def get_vpc_endpoints(client) -> typing.List[dict]:
    paginator = client.get_paginator('describe_vpc_endpoints')
    return list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['VpcEndpoints']))


class VpcCountCheck(QuotaCheck):
    key = "vpc_count"
    description = "VPCs per region"
    scope = QuotaScope.REGION
    service_code = 'vpc'
    quota_code = 'L-F678F1CE'
    used_services = ['ec2']

    @property
    def current(self):
        return len(get_all_vpcs(self.get_client('ec2')))


class InternetGatewayCountCheck(QuotaCheck):
    key = "ig_count"
    description = "VPC internet gateways per region"
    scope = QuotaScope.REGION
    service_code = 'vpc'
    quota_code = 'L-A4707A72'
    used_services = ['ec2']

    @property
    def current(self):
        paginator = self.get_client('ec2').get_paginator('describe_internet_gateways')
        return len(list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['InternetGateways'])))


class NetworkInterfaceCountCheck(QuotaCheck):
    key = "ni_count"
    description = "VPC network interfaces per region"
    scope = QuotaScope.REGION
    service_code = 'vpc'
    quota_code = 'L-DF5E4CA3'
    used_services = ['ec2']

    @property
    def current(self):
        paginator = self.get_client('ec2').get_paginator('describe_network_interfaces')
        return len(list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['NetworkInterfaces'])))


class SecurityGroupCountCheck(QuotaCheck):
    key = "sg_count"
    description = "VPC security groups per region"
    scope = QuotaScope.REGION
    service_code = 'vpc'
    quota_code = 'L-E79EC296'
    used_services = ['ec2']

    @property
    def current(self):
        return len(get_all_sgs(self.get_client('ec2')))


class RulesPerSecurityGroupCheck(InstanceQuotaCheck):
    key = "vpc_rules_per_sg"
    description = "Rules per VPC security group"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-0EA8095F'
    instance_id = 'Security Group ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [sg['GroupId'] for sg in get_all_sgs(get_client(session, 'ec2'))]

    @property
    def current(self):
        try:
            sg = get_sg_by_id(self.get_client('ec2'), self.instance_id)
            return len(sg['IpPermissions']) + len(sg['IpPermissionsEgress'])
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)


class RouteTablesPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_route_tables_per_vpc"
    description = "Route Tables per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-589F43AA'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self):
        if check_if_vpc_exists(self.get_client('ec2'), self.instance_id):
            return len(list(filter(lambda rt: self.instance_id == rt['VpcId'], get_all_rts(self.get_client('ec2')))))
        else:
            raise InstanceWithIdentifierNotFound(self)


class RoutesPerRouteTableCheck(InstanceQuotaCheck):
    key = "vpc_routes_per_route_table"
    description = "Routes per Route Table"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-93826ACB'
    instance_id = 'Route Table ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [rt['RouteTableId'] for rt in get_all_rts(get_client(session, 'ec2'))]

    @property
    def current(self):
        try:
            rt = get_rt_by_id(self.get_client('ec2'), self.instance_id)
            return len(rt['Routes'])
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)


class SubnetsPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_subnets_per_vpc"
    description = "Subnets per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-407747CB'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self):
        if check_if_vpc_exists(self.get_client('ec2'), self.instance_id):
            return len(list(filter(lambda nacl: self.instance_id == nacl['VpcId'], get_all_subnets(self.get_client('ec2')))))
        else:
            raise InstanceWithIdentifierNotFound(self)


class IpsPerSubnetCheck(InstanceQuotaCheck):
    key = "vpc_ips_per_subnet"
    description = "IPs per Subnet"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'D-00000001'
    instance_id = 'Subnet ARN'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [subnet['SubnetArn'] for subnet in get_all_subnets(get_client(session, 'ec2'))]

    @property
    def maximum(self):
        try:
            subnet = get_subnet_by_arn(self.get_client('ec2'), self.instance_id)
            cidr = subnet['CidrBlock']
            return pow(2, 32 - int(cidr.split('/')[1]))
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)

    @property
    def current(self):
        try:
            subnet = get_subnet_by_arn(self.get_client('ec2'), self.instance_id)
            available = int(subnet['AvailableIpAddressCount'])
            return self.maximum - available
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)


class AclsPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_acls_per_vpc"
    description = "Network ACLs per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-B4A6D682'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        if check_if_vpc_exists(self.get_client('ec2'), self.instance_id):
            return len(list(filter(lambda nacl: self.instance_id == nacl['VpcId'], get_all_network_acls(self.get_client('ec2')))))
        else:
            raise InstanceWithIdentifierNotFound(self)


class RulesPerAclCheck(InstanceQuotaCheck):
    key = "vpc_rules_per_acl"
    description = "Rules per Network ACL"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-2AEEBF1A'
    instance_id = 'Network ACL ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [acl['NetworkAclId'] for acl in get_all_network_acls(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        acls = get_all_network_acls(self.get_client('ec2'))
        if self.instance_id in [acl['NetworkAclId'] for acl in acls]:
            return len(next(filter(lambda acl: self.instance_id == acl['NetworkAclId'], acls))['Entries'])
        else:
            raise InstanceWithIdentifierNotFound(self)


class Ipv4CidrBlocksPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_ipv4_cidr_blocks_per_vpc"
    description = "IPv4 CIDR blocks per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-83CA0A9D'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        try:
            vpc = get_vpc_by_id(self.get_client('ec2'), self.instance_id)
            return len(list(filter(lambda cbas: cbas['CidrBlockState']['State'] == 'associated', vpc['CidrBlockAssociationSet'])))
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)


class Ipv6CidrBlocksPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_ipv6_cidr_blocks_per_vpc"
    description = "IPv6 CIDR blocks per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-085A6257'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        try:
            vpc = get_vpc_by_id(self.get_client('ec2'), self.instance_id)
            if 'Ipv6CidrBlockAssociationSet' not in vpc:
                return 0

            return len(list(filter(lambda cbas: cbas['Ipv6CidrBlockState']['State'] == 'associated', vpc['Ipv6CidrBlockAssociationSet'])))
        except KeyError:
            raise InstanceWithIdentifierNotFound(self)


class VpcPeeringConnectionPerVpcCheck(InstanceQuotaCheck):
    key = "vpc_peering_connection_per_vpc"
    description = "Active VPC peering connections per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-7E9ECCDB'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        # Count unique VpcPeeringConnection if either requester or accepter VPC id matches.
        result = set()
        vpc_peerings = get_vpc_peering_connections(self.get_client('ec2'))
        for vpc_peering in vpc_peerings:
            if vpc_peering['Status']['Code'] == 'active' and (
                    vpc_peering['AccepterVpcInfo']['VpcId'] == self.instance_id or
                    vpc_peering['RequesterVpcInfo']['VpcId'] == self.instance_id):
                result.add(vpc_peering['VpcPeeringConnectionId'])
        return len(result)


# I'm not sure what's the scope of this check, so I'll just use ACCOUNT level scope which is the most rigorous.
# This should be a fairly less frequent operation so I'd expect it to be 0 for most of the time.
class OutstandingVpcPeeringConnectionRequests(QuotaCheck):
    key = "vpc_outstanding_vpc_peering_connection_requests"
    description = "Outstanding VPC peering connection requests"
    scope = QuotaScope.ACCOUNT
    service_code = 'vpc'
    quota_code = 'L-DC9F7029'
    used_services = ['ec2']

    @property
    def current(self) -> int:
        # Count unique VpcPeeringConnection if Status Code is 'initiating-request' or 'pending-acceptance'.
        result = set()
        vpc_peerings = get_vpc_peering_connections(self.get_client('ec2'))
        for vpc_peering in vpc_peerings:
            if vpc_peering['Status']['Code'] in ('initiating-request', 'pending-acceptance'):
                result.add(vpc_peering['VpcPeeringConnectionId'])
        return len(result)


class GatewayVpcEndpointsPerRegion(QuotaCheck):
    key = "vpc_gateway_vpc_endpoints_per_region"
    description = "Gateway VPC endpoints per Region"
    scope = QuotaScope.REGION
    service_code = 'vpc'
    quota_code = 'L-1B52E74A'
    used_services = ['ec2']

    @property
    def current(self) -> int:
        return len(list(filter(lambda vpc_endpoint: vpc_endpoint['VpcEndpointType'] == 'Gateway',
                               get_vpc_endpoints(self.get_client('ec2')))))


class InterfaceVpcEndpointsPerVpc(InstanceQuotaCheck):
    key = "vpc_interface_vpc_endpoints_per_vpc"
    description = "Interface VPC endpoints per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'vpc'
    quota_code = 'L-29B6F2EB'
    instance_id = 'VPC ID'
    used_services = ['ec2']

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        return len(list(filter(lambda vpc_endpoint: vpc_endpoint['VpcEndpointType'] == 'Gateway' and vpc_endpoint['VpcId'] == self.instance_id,
                               get_vpc_endpoints(self.get_client('ec2')))))
