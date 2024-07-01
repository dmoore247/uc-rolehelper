
PROFILE=UCDEMO
databricks secrets create-scope uc-rolehelper --profile $PROFILE

databricks secrets put-secret uc-rolehelper config --profile $PROFILE < config.json
