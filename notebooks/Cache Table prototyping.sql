-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.catalog.clearCache()

-- COMMAND ----------

CLEAR CACHE

-- COMMAND ----------

use catalog douglas_moore;
use schema omop;

-- COMMAND ----------

show tables

-- COMMAND ----------

cache table all_visits;

-- COMMAND ----------

select * from all_visits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('select * from all_visits')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.cache()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.count()

-- COMMAND ----------

-- MAGIC %python df.select('*').limit(10).display()

-- COMMAND ----------

-- MAGIC %sql CLEAR CACHE

-- COMMAND ----------

-- MAGIC %python df.select('*').limit(10).display()

-- COMMAND ----------

-- MAGIC %md ## Play with the cache table / clear cache

-- COMMAND ----------

cache table all_visits;

-- COMMAND ----------

-- MAGIC %python df.select('*').limit(10).explain()

-- COMMAND ----------

-- MAGIC %python df.select('*').limit(10).explain()

-- COMMAND ----------

-- MAGIC %sql CLEAR CACHE

-- COMMAND ----------

-- MAGIC %python df.select('*').limit(10).explain()

-- COMMAND ----------

-- MAGIC %python spark.sql("CACHE TABLE all_visits")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

-- MAGIC %python spark.sql("CLEAR CACHE")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

-- MAGIC %md ## Test REVOKE and cached tables

-- COMMAND ----------

SHOW GRANTS on all_visits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```sql
-- MAGIC REVOKE privilege_types ON securable_object FROM principal
-- MAGIC
-- MAGIC privilege_types
-- MAGIC   { ALL PRIVILEGES |
-- MAGIC     privilege_type [, ...] }
-- MAGIC ```

-- COMMAND ----------

REVOKE ALL PRIVILEGES ON all_visits FROM `douglas.moore@databricks.com`;
REVOKE ALL PRIVILEGES ON CATALOG douglas_moore FROM `douglas.moore@databricks.com`;
SHOW GRANTS ON all_visits;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

select * from all_visits

-- COMMAND ----------

GRANT SELECT ON all_visits TO `douglas.moore@databricks.com`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CACHE TABLE all_visits")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("SELECT * from all_visits limit 100").cache()

-- COMMAND ----------

REVOKE SELECT ON all_visits FROM `douglas.moore@databricks.com`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Display works after revoke on a cached table, this is a security gap
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC plan = spark.sql("explain select * from all_visits").collect()[0][0]
-- MAGIC cached = "InMemoryRelation" in plan
-- MAGIC cached, plan

-- COMMAND ----------

-- MAGIC %sql CLEAR CACHE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------


