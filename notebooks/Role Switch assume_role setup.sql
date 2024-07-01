-- Databricks notebook source
-- MAGIC %md # Setup `assume_role` stored function 
-- MAGIC `SELECT role_switch("new_role");` provides SQL interface to role switching service

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS douglas_moore;
CREATE SCHEMA IF NOT EXISTS douglas_moore.roleswitcher ;

-- COMMAND ----------

USE CATALOG douglas_moore;
USE SCHEMA roleswitcher ;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION douglas_moore.roleswitcher.assume_role(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
def trigger(name):
    import requests
    return requests.get('https://gist.githubusercontent.com/dmoore247/3fd4bd8377a42d429f0e0b975db783c1/raw/19372835bc2eb0a68e4d1609332af226251b781c/gist.json').text

return trigger(s) if s else None
$$;

select douglas_moore.roleswitcher.assume_role('acme-role-5')

-- COMMAND ----------

select douglas_moore.roleswitcher.assume_role('acme-role-5')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```
-- MAGIC user 
-- MAGIC    -> role [acme-role-5] -> [securable resource]|GRANTS/permissions| 
-- MAGIC       (catalogs, schemas, tables, views, functions, models, volumes, ... clusters, sql warehouses, workspace folders, repos, models)
-- MAGIC    -> other_groups     -> other securables
-- MAGIC    -> instance profile -> securables
-- MAGIC ```

-- COMMAND ----------


