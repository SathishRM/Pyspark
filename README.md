This repo contains some of my dataframe works in Pyspark.

Please check the file requirements.txt for the list of packages required.

Use spark-submit to run the script, provided adding the pyspark installation directory to PATH variable and have the soure code path in the variable PYTHON_PATH

Example: spark-submit --master local dataframeClient.py

This command executes the spark-submit on the local machine.

To execute it on any cluster, please give the host details to the master agrument.

--> spark-submit --master [host/yarn] --py-files util.zip --files [config_file] dataframeClient.py

To launch the drive program on one of the worker machines inside the cluster, pass DEPLOY_MODE value to the argument deploy mode.

--> spark-submit --master [host/yarn] --deploy-mode cluster --py-files util.zip --files [config_file] dataframeClient.py
