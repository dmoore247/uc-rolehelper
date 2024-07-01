# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW douglas_moore.roleswitcher.roleswitcher_audit_view AS
# MAGIC SELECT user_identity.email, request_params["full_name_arg"] object, response.status_code as status, coalesce(response.error_message,'') as error_message
# MAGIC FROM system.access.audit
# MAGIC WHERE 1=1
# MAGIC AND service_name='unityCatalog'
# MAGIC AND action_name = 'getTable'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from douglas_moore.roleswitcher.roleswitcher_audit_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_identity.email, request_params["full_name_arg"] object, response.status_code as status, coalesce(response.error_message,'') as error_message
# MAGIC FROM system.access.audit
# MAGIC WHERE 1=1
# MAGIC AND service_name='unityCatalog'
# MAGIC AND action_name = 'getTable'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct action_name
# MAGIC FROM system.access.audit
# MAGIC WHERE 1=1
# MAGIC AND service_name='unityCatalog'
# MAGIC AND user_identity.email = "douglas.moore+UC@databricks.com"
# MAGIC order by 1

# COMMAND ----------


