-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create a fake data table, one column for each role and insert data.
-- MAGIC Create a RLS 

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_intent_based_access.default.the_table
(
  role_name STRING not null COMMENT 'role this record is for',
  id bigint,
  id1 BIGINT,
  id2 BIGINT,
  id3 BIGINT,
  id4 BIGINT,
  id5 BIGINT,
  ID6 BIGINT
)
TBLPROPERTIES ('mergeSchema' = 'TRUE')

-- COMMAND ----------

INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-1', id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);
INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-2',id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);
INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-3', id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);
INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-4', id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);
INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-5', id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);
INSERT INTO uc_intent_based_access.default.the_table 
SELECT 'acme-role-6', id, id as id1, id as id2, id as id3, id as id4, id as id5, id as id6 FROM range(10);

-- COMMAND ----------

select * from uc_intent_based_access.default.the_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create RLS and assign it to the Service Principal
-- MAGIC Use this to filter rows based on assigned role.
-- MAGIC
-- MAGIC More complete examples will create a 'permissions' table with a FK into the filtered table(s)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION uc_intent_based_access.default.role_filter(role_param STRING) 
RETURN 
  is_account_group_member(role_param)

-- COMMAND ----------

ALTER FUNCTION uc_intent_based_access.default.role_filter OWNER TO `role-switcher-admin`; 
-- grant access to all user to the function for the demo - don't do it in production

-- COMMAND ----------

ALTER TABLE uc_intent_based_access.default.the_table 
SET ROW FILTER uc_intent_based_access.default.role_filter ON (role_name);

-- COMMAND ----------

-- MAGIC %md ## Row based access controls

-- COMMAND ----------

select * from uc_intent_based_access.default.the_table

-- COMMAND ----------

select role_name, count(id)
from uc_intent_based_access.default.the_table
group by role_name

-- COMMAND ----------


