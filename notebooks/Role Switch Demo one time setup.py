# Databricks notebook source
# MAGIC %md # One time UC Role Switch Demo Setup

# COMMAND ----------

# MAGIC %md ## Setup notes
# MAGIC Groups
# MAGIC - Create group `roleswitchers`, this group has all users and groups associated with role switching
# MAGIC - In Account Admin -> Workspaces -> Permissions, add `roleswitchers` to workspace
# MAGIC - User must belong to a group that has permissions on the working workspace
# MAGIC
# MAGIC 'Roles'
# MAGIC - Roles are Unity Catalog Groups
# MAGIC - Create a UC group for each Role
# MAGIC - Each Role is assigned permissions to securables (Catalogs, Schemas, Shares, Volumes, Tables, Views, Models,...)
# MAGIC - The system is designed such that each 'Role' is complete, membership in a role is 'exclusive'. You may belong to only one Role at a time.
# MAGIC
# MAGIC Securables
# MAGIC - All securables are owned by a service principal in this demo, 'roleswitcher_sp' though doesn't have to be
# MAGIC
# MAGIC Packages
# MAGIC - Make sure to explicitly install `databricks-sdk` 0.7.0 or higher. The default databricks-sdk package is 0.1.6 which is too low to work properly and will quickly return an error.
# MAGIC

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sdk==0.7.0

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

import logging
logging.basicConfig(level=logging.INFO)

from dbx.ucrolehelper import UCRoleHelper
from dbx.ucrolehelper import version as ucrolehelper_version
from dbx.ucrolehelper import config
from databricks.sdk import version as databricks_sdk_version
from databricks.sdk.core import DatabricksError

logging.info(f'dbx.ucrolehelper version: {ucrolehelper_version.__version__}')
logging.info(f'databricks_sdk          version: {databricks_sdk_version.__version__}')

assert databricks_sdk_version.__version__ >= "0.7.0"

from dbx.ucrolehelper.config import role_switcher_admin

# COMMAND ----------


from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.service import iam

import pprint
pp = pprint.PrettyPrinter(indent=4)




# COMMAND ----------

# MAGIC %md ## Enable apps
# MAGIC
# MAGIC ```
# MAGIC curl --netrc --request POST \
# MAGIC  https://accounts.cloud.databricks.com/api/2.0/accounts/${DATABRICKS_ACCOUNT_ID}/oauth2/published-app-integrations \
# MAGIC --header 'Content-Type: application/json' \
# MAGIC --data '{ "app_id": "databricks-cli" }'
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Connect

# COMMAND ----------

logging.info(f"Connecting {config['account_host']}:{config['account_id']}")
u = UCRoleHelper(config=config, roles=roles, assume_username='douglas.moore+UC@databricks.com')

# COMMAND ----------

# MAGIC %md ## Install Service Principal

# COMMAND ----------

service_principal_name = config['sp_display_name']

def create_principal(service_principal_name: str):
    logging.info(f'Creating service principal {service_principal_name}')
    # clean up previous one
    principals = u.a.service_principals.list(filter=f'displayName eq {service_principal_name}')
    for principal in principals:
        u.a.service_principals.delete(id = principal.id)

    try:
        principal = u.a.service_principals.create(
            display_name = service_principal_name
            )
    except DatabricksError as e:
        logging.warning(f'Databricks Errror on create service principal {service_principal_name} : {e}')


principals = u.a.service_principals.list(filter=f'displayName eq {service_principal_name}')
logging.info(principals)

groups = u.a.groups.list(filter=f'displayName co {roles[0][10:]}')
logging.info(groups)

# COMMAND ----------

# MAGIC %md ## Setup demo catalog

# COMMAND ----------

from dbx.ucrolehelper.config import catalog_name
from databricks.sdk.service import catalog

try:
    logging.info(f'Deleting catalog {catalog_name}')
    u.w.catalogs.delete(name=catalog_name, force=True)
except DatabricksError as e:
    logging.warning(f'Databricks Errror on delete catalog {catalog} : {e}')

try:
    logging.info(f'Creating catalog {catalog_name}')
    u.w.catalogs.create(name=catalog_name, comment="Role switch demo")
except DatabricksError as e:
    logging.warning(f'Databricks Errror on create catalog {catalog} : {e}')
    
try:
    logging.info(f'Changing owner catalog {catalog_name} to {role_switcher_admin}')
    u.w.catalogs.update(name=catalog_name, owner=role_switcher_admin)
except DatabricksError as e:
    logging.warning(f'Databricks Errror on update catalog {catalog} owner {role_switcher_admin}: {e}')

try:
    u.w.grants.update(
        securable_type=catalog.SecurableType.CATALOG, 
        full_name=catalog_name,
        changes=[
                catalog.PermissionsChange(add=[catalog.Privilege.USE_CATALOG],
                                            principal=role_switcher_admin)
                    ])
except DatabricksError as e:
    logging.warning(f'Databricks Errror on grant on catalog {catalog} : {e}')


# COMMAND ----------

# MAGIC %md ## Setup Demo Roles, Groups, Schemas, tables etc...

# COMMAND ----------

for role in roles:
    
    try:
        logging.info(f'Create group {role}')
        u.a.groups.create(display_name=role)
    except DatabricksError as e:
       logging.warning(f'Databricks Errror on group create {role} : {e}')
    try:
        logging.info(f'Create schema {catalog_name}.{role}')
        u.w.schemas.create(name=role, catalog_name=catalog_name, comment="Role switch demo")
    except DatabricksError as e:
       logging.warning(f'Databricks Errror on schema create {role} : {e}')

    # update owner
    schema_name = f'{catalog_name}.{role}'
    try:
        logging.info(f'Update owner {schema_name} TO {role_switcher_admin}')
        u.w.schemas.update(full_name=schema_name, owner=role_switcher_admin)
    except DatabricksError as e:
        logging.warning(f'Databricks Errror on update schema {schema_name} owner {role_switcher_admin}: {e}')

    # update grants
    try:
        logging.info(f'Update grants {schema_name} TO {role_switcher_admin}')
        u.w.grants.update(
        securable_type=catalog.SecurableType.SCHEMA, 
        full_name=schema_name,
        changes=[
                catalog.PermissionsChange(
                    add=[catalog.Privilege.ALL_PRIVILEGES],
                    principal=role_switcher_admin)
                ])
    except DatabricksError as e:
        logging.warning(f'Databricks Errror on schema grant {role} : {e}')
        
    # create sample table
    table_name = 'the_table'
    try:
        logging.info(f'CTAS {catalog_name}.{role}.{table_name}')
        _ = u.w.statement_execution.execute_statement(
            warehouse_id=config['warehouse_id'],
            catalog=catalog_name,
            schema=role,
            statement=f"CREATE OR REPLACE TABLE {table_name} AS SELECT '{role}' AS role_name, id FROM range(10)"
            )
        if _.status == 'FAILED':
            logging.warning(f'CREATE TABLE {table_name} failed: {_.error}')
        
        logging.info(f'SELECT {catalog_name}.{role}.{table_name}')
        _ = u.w.statement_execution.execute_statement(
            warehouse_id=config['warehouse_id'],
            catalog=catalog_name,
            schema=role,
            statement=f"SELECT * from TABLE {table_name}"
            )
        if _.status == 'FAILED':
            logging.warning(f'SELECT TABLE {table_name} failed: {_.error}')
    except DatabricksError as e:
        logging.warning(f'Databricks Errror on create table {catalog_name}.{schema_name}.{table_name} : {e}')
    
    # update owner
    full_name = f'{catalog_name}.{role}.the_table'
    try:
        logging.info(f'ALTER TABLE {full_name} SET OWNER TO {role_switcher_admin}')
        u.w.tables.update(full_name=full_name, owner=role_switcher_admin)
    except DatabricksError as e:
        logging.warning(f'Databricks Errror on update table {full_name} owner {role_switcher_admin}: {e}')
