This repo contains some of my dataframe works in Pyspark.

Please check the file requirements.txt for the list of packages required.

Use spark-submit to run the script, provided adding the pyspark installation directory to PATH variable and have the soure code path in the variable PYTHON_PATH

Example: spark-submit --master local dataframeClient.py
This command executes the spark-submit on the local machine.

To execute it on any cluster, please give the host details to the master agrument.
Example: spark-submit --master spark://[hostname]:[port#] dataframeClient.py

To launch the drive program on one of the worker machines inside the cluster, pass DEPLOY_MODE value to the argument deploy mode.
Example: spark-submit --master spark://[hostname]:[port#] --deploy-mode DEPLOY_MODE dataframeClient.py
