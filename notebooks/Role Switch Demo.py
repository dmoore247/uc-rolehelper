# Databricks notebook source
# MAGIC %run "./Role Switch Setup"

# COMMAND ----------

# MAGIC %md #Intent, Purpose or Role based access controls
# MAGIC
# MAGIC Use Cases:
# MAGIC - Clinical trial data, strict separation of clinical trial data from other data is required
# MAGIC - Partner or customer data and multi-tenancy. Separation of customer data from one to the other is scrictly mandated
# MAGIC - Regulations or contractual obligations require strict separation or isolation based on your purpose or intent.
# MAGIC
# MAGIC Details:
# MAGIC - Most governance schemes are '**and**' based (this 'and' that), this one is '**or**' based (this 'or' that)
# MAGIC - We implement these controls with a few underlying groups, e.g. 'role1, role2, role3, role4,...', each group represents exclusive permissions. The user switches from one group to another. The user may exist in only one group at a time. Each group or 'role' represents one study or client or unit of isolation.
# MAGIC
# MAGIC - The group is given permissions on the underlying Unity Catalog securables (Catalogs, schemas, tables, views, functions, shares, models etc)
# MAGIC
# MAGIC - This approach will work across Interactive clusters, Jobs, SQL Warehouses.
# MAGIC - Below is a demo of one of several possible user interfaces
# MAGIC
# MAGIC Corner Cases:
# MAGIC - Group access is a permission, which is cached system wide for performance purposes. It will take a few seconds for the change group to take affect. Within the same notebook, the switch is synchronous
# MAGIC

# COMMAND ----------

# MAGIC %md # Demo

# COMMAND ----------

# MAGIC %md ## Select a Role

# COMMAND ----------

pickRole()

# COMMAND ----------

# MAGIC %md ## Verify the changes

# COMMAND ----------

# MAGIC %sql select
# MAGIC   is_account_group_member('acme-role-1') r1,
# MAGIC   is_account_group_member('acme-role-2') r2,
# MAGIC   is_account_group_member('acme-role-3') r3,
# MAGIC   is_account_group_member('acme-role-4') r4,  
# MAGIC   is_account_group_member('acme-role-5') r5,  
# MAGIC   is_account_group_member('acme-role-6') r6,
# MAGIC   now(),
# MAGIC   current_user()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS WITH USER `douglas.moore+UC@databricks.com`

# COMMAND ----------

# MAGIC %md ### Check table access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Will fail with AnalysisException: [INSUFFICIENT_PERMISSIONS] if user is not member of the role
# MAGIC select * from uc_intent_based_access.`acme-role-1`.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.`acme-role-2`.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.`acme-role-3`.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.`acme-role-4`.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.`acme-role-5`.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.`acme-role-6`.the_table

# COMMAND ----------

# MAGIC %md ## Clear Cache

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE uc_intent_based_access.`acme-role-3`.the_table;
# MAGIC
# MAGIC
# MAGIC EXPLAIN SELECT * from uc_intent_based_access.`acme-role-3`.the_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from uc_intent_based_access.`acme-role-3`.the_table;

# COMMAND ----------

pickRole()

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN SELECT * from uc_intent_based_access.`acme-role-3`.the_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from uc_intent_based_access.`acme-role-3`.the_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #RLS testing

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED uc_intent_based_access.default.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_intent_based_access.default.the_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select role_name, count(id)
# MAGIC from uc_intent_based_access.default.the_table
# MAGIC group by role_name

# COMMAND ----------

# MAGIC %md ## Audit Log review
# MAGIC
# MAGIC Visit the Role Swithcher Audit [dashboard](https://uc-demo.cloud.databricks.com/dashboardsv3/01ef37c3eb8d16eeb449afa1d6301e17/published?o=2628768189666277)
