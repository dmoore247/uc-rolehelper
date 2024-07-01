# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.25.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import ipywidgets as widgets
from ipywidgets import interactive
from dbx.ucrolehelper import UCRoleHelper
from dbx.ucrolehelper import version as ucrolehelper_version
from dbx.ucrolehelper import UCRHConfig
from databricks.sdk import version as databricks_sdk_version
import logging
logging.basicConfig(level=logging.WARN)

logging.info(f'dbx.ucrolehelper version: {ucrolehelper_version.__version__}')
logging.info(f'databricks_sdk          version: {databricks_sdk_version.__version__}')

# COMMAND ----------

config = UCRHConfig().get_config()
roles = config.get('roles')

ucrh = UCRoleHelper(config=config, roles=roles, assume_username=None, dbutils=dbutils)

default_role = ''
current_role = default_role
roles.append(default_role)

#DropDown

def do_switch(new_role:str):
    print(f'New role {new_role} ... clearing cached data.')
    spark.sql("CLEAR CACHE")
    ucrh.switch_role(new_role = new_role)
    print(ucrh.get_current_user_roles())

def pickRole():
    options = roles
    roleSelect=widgets.Dropdown(
        options=roles,
        value=current_role,
        description='Role:',
    )
    
    output_role = widgets.Output()
    display(roleSelect,output_role)
    def on_value_change(change):
        with output_role:
            do_switch(new_role=roleSelect.value)

    roleSelect.observe(on_value_change, names='value')
