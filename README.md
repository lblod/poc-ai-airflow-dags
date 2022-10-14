# poc-ai-airflow-dags
The code in this repository is a part of the poc-ai-* namespace. This namespace contains the poc for the automatic classification and extraction of relevant information from documents in the [local decisions and linked open data space](https://github.com/lblod). You can find more high level information in the [poc-ai-deployment](https://github.com/lblod/poc-ai-deployment) repository.

## Table of content
[TOC]

## Quick start
This reposity contains the information on how to change the provided DAGs and how to apply your custom config.

In order to clone this repository you can run the following command:
```
git clone https://github.com/lblod/poc-ai-airflow-dags.git
```

Once you have cloned the repository and moved it to correct folder for airflow (see [airflow documentation](https://https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)), you should be able to see the following DAGs in your Airflow interface.

![screenshot of airflow interface](https://github.com/lblod/poc-ai-airflow-dags/images/Airflow_DAGs_screenshot.png)

If you get the same interface as the screenshot above, you can start running the dags

## Customization of DAGs
In case you want to customize certain setups, you should not make changes to this repository. This repository can be seen as the interpreter on what to execute for the airflow DAGs. If you would want to adapt the configuration to your own needs, you will have to execute the following steps:
1. Duplicate the content the [config/ folder](https://github.com/lblod/poc-ai-airflow-dags/tree/master/configs)
2. Adapt it to your needs (do not change file names, only adapt the values in the `*.yaml` files)
3. Mount the new config into `poc-ai-deployment-airflow-worker-1`, keep track of what folder you mounted the new configs in.
4. Set an Airflow variable (under `Admin > Variables`)  to the path you mounted your new configs in. AFter the DAG refresh, your DAGs should be reloaded from the mounted config.
5. Verify if everything worked as intended by waiting for the DAG refresh and validating that you did not make an error.

## Information about configs

 :::info
 will be added later

 :::



