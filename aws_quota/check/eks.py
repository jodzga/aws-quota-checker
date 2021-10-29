from .quota_check import QuotaCheck, QuotaScope


class ClusterCountCheck(QuotaCheck):
    key = "eks_count"
    description = "EKS Clusters per region"
    scope = QuotaScope.REGION
    service_code = 'eks'
    quota_code = 'L-1194D53C'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_clusters()['clusters'])
