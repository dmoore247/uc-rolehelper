import json
from databricks.sdk import WorkspaceClient

class UCRHConfig:

    config = None
    roles = None
    catalog_name = None
    role_switcher_admin = None
    profile = None
    
    def __init__(self, profile:str = "UCDEMO"):
        self.profile = profile
    
    def get_config(self) -> dict:
        
        w = WorkspaceClient(profile=self.profile)
        dbutils = w.dbutils
        self.config = json.loads(dbutils.secrets.get('uc-rolehelper','config'))


        # roles / groups
        # Populate this array with the groups representing the roles users can assume.
        self.roles = self.config.get('roles')

        self.catalog_name = self.config.get('catalog_name')

        # group to administer role switching
        self.role_switcher_admin = self.config.get('role_switcher_admin')
        self.validate()

        return self.config

    def validate(self):
        assert self.config is not None
        assert self.roles is not None
        assert self.catalog_name is not None
        assert self.role_switcher_admin is not None

        assert len(self.config.get('account_host')) > 16
        assert self.config.get('account_host')[:8] == "https://"
        assert self.config.get('host')[:8] == "https://"
        assert len(self.config.get('account_id')) == 36
        assert len(self.config.get('client_id')) == 36
        assert len(self.config.get('sp_application_id')) == 36
        assert len(self.config.get('sp_id')) == 16
        assert len(self.config.get('client_secret')) == 36
        assert str(self.config.get('client_secret'))[:4] == "dose"
