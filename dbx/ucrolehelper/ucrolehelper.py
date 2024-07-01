import logging

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.service.iam import User


class UCRoleHelper:
    def __init__(self, config: dict, roles: list, assume_username: str = None, dbutils = None):
        """
        config: account_host, account_id, client_id, client_secret for databricks account service principal
        roles: list of possible roles (account groups) user can assume
        assumer_username: the username to assume
        """
        assert config is not None
        assert roles is not None
        assert len(roles) > 1
        #assert assume_username is not None
        self.dbutils = dbutils

        config = config.copy()
        self.roles = roles
        self.assume_username = assume_username
        self.assume_user_id = None
        self.me = None

        self.a = AccountClient(
            host=config["account_host"],
            account_id=config["account_id"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            debug_headers=False,
            debug_truncate_bytes=1024,
        )

        self.w = WorkspaceClient(
            host=config["host"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            debug_headers=False,
            debug_truncate_bytes=1024,
        )

    def get_account(self) -> AccountClient:
        return self.a

    def get_current_user_id(self) -> str | None:
        """Given the assume username, get the user_id"""
        if not self.assume_user_id:
            if self.assume_username:
                users = self.a.users.list(attributes="id", filter=f"userName eq {self.assume_username}")
                self.assume_user_id = users[0].id if users else None
            else:
                self.assume_user_id = WorkspaceClient(
                    host = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
                    token=self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()).current_user.me().id
        return self.assume_user_id

    def get_me(self) -> User:
        if not self.me:
            self.me = self.a.users.list(filter=f"userName eq {self.assume_username}")[0]
            logging.debug(f"me: {self.me}")
        return self.me

    def get_available_groups(self):
        filter_exp = ""
        for role_name in self.roles:
            if role_name != '':
                if len(filter_exp) > 0:
                    filter_exp += " or "
                filter_exp += f"displayName eq '{role_name}'"
        assert len(filter_exp) > 4
        return self.a.groups.list(filter=filter_exp)

    def get_current_user_roles(self):
        """Get the current role assignment for the user"""
        roles = []
        user_id = self.get_current_user_id()
        assert user_id is not None
        for group in self.get_available_groups():
            if group.members:
                for member in group.members:
                    logging.debug(f"get_current_user_roles: assume_user_id {user_id} Member {member}")
                    if member.value == user_id:
                        logging.debug(f"get_current_user_roles: found user in group {group.display_name}")
                        roles.append(group.display_name)
        logging.debug(f"get_current_user_roles: returning roles {roles}")
        return list(roles)

    def get_role_ids(self):
        """Given possible 'roles' obtain all the corresponding group ids"""

        role_map = {}
        for group in self.get_available_groups():
            role_map[group.display_name] = group.id

        return role_map

    def get_group_id_from_role(self, role: str) -> str:
        role_map = self.get_role_ids()
        return role_map[role]

    def has_role(self, role: str) -> bool:
        self.get_current_user_id()
        group_id = self.get_group_id_from_role(role)
        group = self.a.groups.get(group_id)
        if group.members:
            for member in group.members:
                if member.display == role:
                    return True
        return False

    def add_user_to_role(self, role: str):
        """add memeber to a role group"""
        group_id = self.get_group_id_from_role(role)
        user_id = self.get_current_user_id()
        logging.info(f"Adding user {user_id} to group {group_id} for role {role}")
        self.a.groups.patch(
            group_id,
            operations=[iam.Patch(op=iam.PatchOp.ADD, value={"members": [{"value": user_id}]})],
        )

    def remove_user_from_role(self, role: str):
        """Remove assumed user from group"""
        user_id = self.get_current_user_id()
        group_id = self.get_group_id_from_role(role)

        users_to_remove = [user_id]
        logging.debug(f"remove_user_from_role: user_id {user_id} from role {role}")
        operations = [
            iam.Patch(op=iam.PatchOp.REMOVE, path=f'members[value eq "{x}"]')
            for x in users_to_remove
        ]

        for op in operations:
            logging.debug(op.as_dict())

        schemas = [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
        self.a.groups.patch(group_id, operations=operations, schemas=schemas)

    def remove_user_from_all_roles(self):
        """ Remove user from all roles """
        user_roles = self.get_current_user_roles()
        logging.debug(f"remove_user_from_all_roles: roles found {user_roles}")
        for role in user_roles:
            self.remove_user_from_role(role)

    def switch_role(self, new_role:str):
        """
        Switch user from the existing role(s) to the new role 
        by removing user from current role(s) and adding to the new role.
        The role must be within the list of config.roles
        """
        current_roles = self.get_current_user_roles()
        if new_role is not None and new_role in self.roles and new_role not in current_roles:
            self.remove_user_from_all_roles()
            if new_role and len(new_role)>2:
                self.add_user_to_role(new_role)
