# Databricks notebook source
# MAGIC %md
# MAGIC ## Passing secrets in mlflow pyfunc models
# MAGIC
# MAGIC tags: roleswitcher
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.15.0 mlflow==2.9.2 mlflow-skinny[databricks]>=2.9.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

SCOPE = "uc-rolehelper"
SECRET = "tester"
CATALOG = "douglas_moore"
SCHEMA  = "roleswitcher"
MODEL_NAME = "assume_role_pyfunc"
RUN_NAME = "secret_test"
MODEL_DESCRIPTION = "pyfunc endpoint housing uc-rolehelper switching logic"

# COMMAND ----------

from mlflow.pyfunc import PythonModel
from mlflow.pyfunc.model import PythonModelContext
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
import requests
import base64

# COMMAND ----------

# Model wrapper class
import databricks
from databricks.sdk import WorkspaceClient

class SecretTest(PythonModel):

    def __init__(self, secret):
        self.init_secret = secret

    def load_context(self, context):
        ws = WorkspaceClient()
        self.ws_secret = base64.b64decode(ws.secrets.get_secret(SCOPE, SECRET).value).decode("utf-8")

    def predict(self, context: PythonModelContext,
                      model_input,
                      params: Optional[Dict[str, Any]] = {'temperature': 0.01}):

        return [(self.init_secret).upper() + " " + self.ws_secret.upper()]

# COMMAND ----------

from mlflow.pyfunc.model import PythonModelContext
from mlflow.models.signature import ModelSignature 
from mlflow.types.schema import Schema, ColSpec
model_config = {}
input_schema = Schema(
       [
           ColSpec(name="user", type="string"),
           ColSpec(name="role", type="string"),

       ]
)
output_schema = Schema([ColSpec(name="status", type="string")])
signature = ModelSignature(inputs=input_schema, outputs=output_schema)
pythonModelContext = PythonModelContext(model_config={}, artifacts={})

# COMMAND ----------

#local test:
from mlflow.pyfunc.model import PythonModelContext
secretTest = SecretTest(secret=dbutils.secrets.get(SCOPE, SECRET))
pythonModelContext = PythonModelContext(model_config=model_config, artifacts={})
secretTest.load_context(pythonModelContext)

# COMMAND ----------

secretTest.predict(context = pythonModelContext, model_input = "")

# COMMAND ----------

# Saving to endpoint:
import mlflow
from mlflow.models import infer_signature
with mlflow.start_run(run_name=RUN_NAME, description=MODEL_DESCRIPTION) as run:
    #signature = infer_signature(model_input, model_output)
    pip_requirements = ["mlflow[gateway]==2.9.2", "pandas", "numpy"]
    mlflow.pyfunc.log_model(RUN_NAME, 
                            python_model=secretTest,
                            signature=signature,
                            pip_requirements=["databricks-sdk==0.15.0", "base64"]
                            )
    run_id = run.info.run_id

print(run.info.run_id)

# COMMAND ----------

# MAGIC %md ## Register the model
# MAGIC Register the model to Unity Catalog

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS douglas_moore.roleswitcher

# COMMAND ----------

# Register model to unity catalog

mlflow.set_registry_uri("databricks-uc")
tags = {"project":"uc-roleswitcher"}
mlflow.register_model(
    f"runs:/{run.info.run_id}/secret_test", 
    f"{CATALOG}.{SCHEMA}.{MODEL_NAME}",
    tags=tags)

# COMMAND ----------


