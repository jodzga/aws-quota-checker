from .quota_check import QuotaCheck, QuotaScope


class MeshCountCheck(QuotaCheck):
    key = "am_mesh_count"
    description = "App Meshes per account"
    scope = QuotaScope.ACCOUNT
    service_code = 'appmesh'
    quota_code = 'L-AC861A39'
    used_services = [service_code]

    @property
    def current(self):
        return len(self.get_client(self.service_code).list_meshes()['meshes'])
