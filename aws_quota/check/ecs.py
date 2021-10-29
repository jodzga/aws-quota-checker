from .quota_check import QuotaCheck, QuotaScope


class ClusterCountCheck(QuotaCheck):
    key = "ecs_count"
    description = "ECS Clusters per region"
    scope = QuotaScope.REGION
    service_code = 'ecs'
    quota_code = 'L-21C621EB'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_clusters()['clusterArns'])
