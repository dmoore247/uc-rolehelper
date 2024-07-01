import logging
import unittest

from dbx.ucrolehelper import UCRHConfig, UCRoleHelper

# logging.getLogger("databricks.sdk").setLevel(logging.WARN)


logging.basicConfig(level=logging.WARN)


class TestConfig(unittest.TestCase):
    roles = None
    email = "douglas.moore+UC@databricks.com"  # test user account
    config = None

    def test_config(self):
        self.config = UCRHConfig(profile="UCDEMO").get_config()
        self.assertIsNotNone(self.config)
        self.roles = self.config.get("roles")
        self.assertIsNotNone(self.roles)
        self.assertGreaterEqual(len(self.roles), 5)
        self.assertTrue(isinstance(self.config, dict))
        u = UCRoleHelper(config=self.config, roles=self.roles, assume_username=self.email)
        self.assertTrue(isinstance(u, UCRoleHelper))


class TestUCRoleHelper(unittest.TestCase):
    roles = None
    email = "douglas.moore+UC@databricks.com"  # test user account
    config = None

    def setUp(self):
        self.config = UCRHConfig().get_config()
        self.roles = self.config.get("roles")
        self.u = UCRoleHelper(config=self.config, roles=self.roles, assume_username=self.email)

    def test_constructor(self):
        self.assertIsNotNone(self.u)
        self.assertEqual(len(self.roles), len(self.u.roles))
        self.assertIsNotNone(self.u.a)
        self.assertIsNotNone(self.u.w)

    def test_me(self):
        me = self.u.get_me()
        self.assertIsNotNone(me.id)
        self.assertEqual(self.email, me.user_name)
        self.assertEqual(True, me.active)

    def test_get_current_user_id(self):
        self.assertTrue(self.u.get_current_user_id())

    def test_current_user(self):
        self.assertIsNotNone(self.u.get_current_user_id())

    def test_get_current_role(self):
        self.u.remove_user_from_all_roles()
        self.assertEqual(0, len(self.u.get_current_user_roles()))

    def test_get_available_groups(self):
        groups = self.u.get_available_groups()
        self.assertEqual(len(groups), len(self.u.roles), [self.u.roles, groups])

    def test_get_role_ids(self):
        role_map = self.u.get_role_ids()
        self.assertEqual(len(self.roles), len(role_map))
        self.assertTrue(self.roles[0] in role_map)
        self.assertTrue(self.roles[1] in role_map)
        self.assertTrue(self.roles[2] in role_map)
        self.assertIsNotNone(role_map)

    def test_is_member(self):
        self.assertFalse(self.u.has_role(self.roles[2]))

    def test_add_user(self):
        self.u.add_user_to_role(role=self.roles[0])
        user_roles = self.u.get_current_user_roles()
        self.assertTrue(self.roles[0] in user_roles)
        self.assertTrue(set(self.roles) & set(user_roles))  # should have some intersection

    def test_remove_user(self):
        myrole = self.roles[0]
        self.u.add_user_to_role(role=myrole)
        self.u.remove_user_from_role(role=myrole)
        self.assertTrue(myrole not in self.u.get_current_user_roles())

    def test_add_role(self):
        logging.debug("test_add_role - remove all")
        self.u.remove_user_from_all_roles()
        current_roles = self.u.get_current_user_roles()
        self.assertIsNotNone(self.u.assume_user_id)

        self.assertEqual(
            0, len(current_roles), f"Should be no current roles, current roles= {current_roles}"
        )

        logging.debug("test_add_role - add user to role")
        self.u.add_user_to_role(role=self.roles[0])
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(
            1, len(current_roles), f"Should be one current role, current roles= {current_roles}"
        )
        self.assertTrue(self.roles[0] in current_roles, f"current roles {current_roles}")

    def test_remove_from_all(self):
        self.u.add_user_to_role(role=self.roles[0])
        current_roles = self.u.get_current_user_roles()
        self.assertGreaterEqual(len(current_roles), 1, msg="Must be member of one or more roles")

        self.u.remove_user_from_all_roles()
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), "No current roles")

        self.u.add_user_to_role(role=self.roles[0])
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(len(current_roles), 1, "One current role")

    def test_switch_roles(self):
        self.u.get_current_user_id()
        self.u.remove_user_from_all_roles()
        self.u.add_user_to_role(role=self.roles[0])
        self.u.switch_role(new_role=self.roles[1])
        current_roles = self.u.get_current_user_roles()
        self.assertTrue(self.roles[1] in current_roles, str(current_roles))
        self.assertTrue(self.roles[0] not in current_roles, str(current_roles))

    def test_switch_roles_bad_input_none(self):
        """Reject or ignore None"""
        self.u.remove_user_from_all_roles()
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")

        self.u.switch_role(new_role=None)
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")

    def test_switch_roles_bad_input_empty(self):
        """Reject or ignore ''"""
        self.u.remove_user_from_all_roles()
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")

        self.u.switch_role(new_role="")
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")

    def test_switch_roles_bad_input_rolename(self):
        """Reject or ignore 'badrolename'"""
        self.u.remove_user_from_all_roles()
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")

        self.u.switch_role(new_role="bad rolename")
        current_roles = self.u.get_current_user_roles()
        self.assertEqual(0, len(current_roles), f"should have zero, current roles {current_roles}")


if __name__ == "__main__":
    unittest.main(verbosity=True)
