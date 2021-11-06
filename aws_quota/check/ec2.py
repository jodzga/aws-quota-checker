from aws_quota.utils import get_client
from .quota_check import QuotaCheck, InstanceQuotaCheck, QuotaScope
from .vpc import get_all_vpcs
import typing
import boto3
from aws_quota import threadsafecache


@threadsafecache.run_once_cache
def get_all_running_ec2_instances(client):
    return [instance for reservations in client.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                }
            ]
            )['Reservations'] for instance in reservations['Instances']]


@threadsafecache.run_once_cache
def get_all_spot_requests(client):
    return client.describe_spot_instance_requests()[
        'SpotInstanceRequests']


class OnDemandStandardInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_standard_count"
    description = "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-1216C47A"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['a', 'c', 'd', 'h', 'i', 'm', 'r', 't', 'z'], instances)))


class OnDemandFInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_f_count"
    description = "Running On-Demand F EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-74FC7D96"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['f'], instances)))


class OnDemandGInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_g_count"
    description = "Running On-Demand G EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-DB2E81BA"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['g'], instances)))


class OnDemandInfInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_inf_count"
    description = "Running On-Demand Inf EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-1945791B"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['inf'], instances)))


class OnDemandPInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_p_count"
    description = "Running On-Demand P EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-417A185B"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['p'], instances)))


class OnDemandXInstanceCountCheck(QuotaCheck):
    key = "ec2_on_demand_x_count"
    description = "Running On-Demand X EC2 instances"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-7295265B"
    used_services = [service_code]

    @property
    def current(self):
        instances = get_all_running_ec2_instances(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['InstanceType'][0] in ['x'], instances)))


class SpotStandardRequestCountCheck(QuotaCheck):
    key = "ec2_spot_standard_count"
    description = "All Standard (A, C, D, H, I, M, R, T, Z) EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-34B43A08"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['a', 'c', 'd', 'h', 'i', 'm', 'r', 't', 'z'], requests)))


class SpotFRequestCountCheck(QuotaCheck):
    key = "ec2_spot_f_count"
    description = "All F EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-88CF9481"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['f'], requests)))


class SpotGRequestCountCheck(QuotaCheck):
    key = "ec2_spot_g_count"
    description = "All G EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-3819A6DF"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['g'], requests)))


class SpotInfRequestCountCheck(QuotaCheck):
    key = "ec2_spot_inf_count"
    description = "All Inf EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-B5D1601B"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['inf'], requests)))


class SpotPRequestCountCheck(QuotaCheck):
    key = "ec2_spot_p_count"
    description = "All P EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-7212CCBC"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['p'], requests)))


class SpotXRequestCountCheck(QuotaCheck):
    key = "ec2_spot_x_count"
    description = "All X EC2 Spot Instance Requests"
    scope = QuotaScope.ACCOUNT
    service_code = "ec2"
    quota_code = "L-E3A00192"
    used_services = [service_code]

    @property
    def current(self):
        requests = get_all_spot_requests(self.get_client(self.service_code))

        return len(list(filter(lambda inst: inst['LaunchSpecification']['InstanceType'][0] in ['x'], requests)))


class ElasticIpCountCheck(QuotaCheck):
    key = "ec2_eip_count"
    description = "EC2 VPC Elastic IPs"
    scope = QuotaScope.ACCOUNT
    service_code = 'ec2'
    quota_code = 'L-0263D0A3'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_addresses()['Addresses'])


class TransitGatewayCountCheck(QuotaCheck):
    key = "ec2_tgw_count"
    description = "Transit Gateways per account"
    scope = QuotaScope.ACCOUNT
    service_code = 'ec2'
    quota_code = 'L-A2478D36'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_transit_gateways()['TransitGateways'])


class VpnConnectionCountCheck(QuotaCheck):
    key = "ec2_vpn_connection_count"
    description = "VPN connections per region"
    scope = QuotaScope.REGION
    service_code = 'ec2'
    quota_code = 'L-3E6EC3A3'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).describe_vpn_connections()['VpnConnections'])


class AttachmentsPerVpc(InstanceQuotaCheck):
    key = "vpc_attachments_per_vpc"
    description = "Attachments per VPC"
    scope = QuotaScope.INSTANCE
    service_code = 'ec2'
    quota_code = 'L-6DA43717'
    instance_id = 'VPC ID'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        return [vpc['VpcId'] for vpc in get_all_vpcs(get_client(session, 'ec2'))]

    @property
    def current(self) -> int:
        paginator = self.get_client('ec2').get_paginator('describe_transit_gateway_vpc_attachments')
        return len(list(filter(lambda attachment: self.instance_id == attachment['VpcId'],
            list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['TransitGatewayVpcAttachments'])))))


class RoutesPerClientVPNEndpoint(InstanceQuotaCheck):
    key = "vpc_routes_per_client_vpn_endpoint"
    description = "Routes per Client VPN endpoint"
    scope = QuotaScope.INSTANCE
    service_code = 'ec2'
    quota_code = 'L-401D78F7'
    instance_id = 'Client VPN Endpoint ID'
    used_services = [service_code]

    @staticmethod
    def get_all_identifiers(session: boto3.Session) -> typing.List[str]:
        paginator = get_client(session, 'ec2').get_paginator('describe_client_vpn_endpoints')
        vpn_endpoints = list((chunk for page in paginator.paginate(PaginationConfig={'PageSize': 100}) for chunk in page['ClientVpnEndpoints']))
        return [vpn_endpoint['ClientVpnEndpointId'] for vpn_endpoint in vpn_endpoints]

    @property
    def current(self) -> int:
        paginator = self.get_client('ec2').get_paginator('describe_client_vpn_routes')
        return len(list((chunk for page in paginator.paginate(ClientVpnEndpointId=self.instance_id, PaginationConfig={'PageSize': 100}) for chunk in page['Routes'])))
