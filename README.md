# Cloud ETL and Batch Processing

## ETL in Cloud
In this big data era, data is growing very fast. Data integration
from many sources and in huge amount is inevitable. Managing such
complex data is costly in terms of engineering bandwidth, physical
data warehouse or data center.

A way to overcome these challenges is by using Cloud ETL Tools which
provides robust ETL pipelines and powerful hardware to be used so that
companies don't need to invest on capital expenditure. Some key
advantages of using Cloud ETL:

* Cost-effective compared to buy machines
* Quick insights provided
* Easy setup

## Batch Processing
Batch workload processing refers to groups of jobs (batches) that are 
scheduled to be processed at the same time. Traditionally, batch 
workload usually performed when CPU usage is low (typically overnight)
because it requires high CPUs and used to process the closing of
a business day.

Today, batch processing is done by job schedulers, batch processing
system, workload automation solutions, and applications native to operating systems.
Batch processing applied on cloud will tackle the resource-intensive
problem and make it easier to orchestrate jobs. In this project,
I use one of the most popular cloud service, i.e. 
[Google Cloud Platform (GCP)](https://cloud.google.com/).

## Google Cloud Platform Setup
There are some setup on tools that needed to be done before using
GCP for ETL. GCP provides $300 free trial credit for 3 months so
after applying the free trial, things that you need to set up are:

1. Service Accounts. Make sure that Service Account used has **Owner** role
   to enable all Google Services.
2. Google Cloud Composer. This is a fully managed workflow orchestration service
   built in popular Apache Airflow open source project and operated using the Python programming language.
   Create an environment here by using the Service Account that has Owner role.
   For complete steps please refer [this page](https://cloud.google.com/composer/docs/how-to/managing/creating).
3. Airflow Web UI. In GCP we use Airflow to schedule workflow. 
   After environment is created, go to Environment Configuration then click the Airflow web UI. 
   Set up variables that will be used during ETL process in Admin -> Variables. Set: 
   1. `bucket_path` (path of your created bucket)
   2. `project_id` (your project id)
4. Google Cloud Storage. Create a bucket in Google Cloud Storage to store data sources and additional files
   needed. For complete steps please refer [this page](https://cloud.google.com/composer/docs/how-to/using/using-dataflow-template-operator)
   
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1eLDSktNDPShVyRoPeQcC2ySiOY-rItc2">
</div><br />

## Apache Airflow
Apache Airflow is a platform to programmatically author, schedule, and monitor workflows.
Workflows are implemented as directed acyclic graphs (DAGs) of tasks. With Airflow, you can
schedule your tasks and specify dependencies among them. Pipelines also generated to monitor
tasks status and troubleshoot problem if needed.

<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1C4eqGhJLqf3-iYfVt0NLz67gOvBluYkd">
<small> Airflow Web UI </small>
</div><br />

### Writing DAGs
There are only 5 steps you need to remember to write an Airflow DAG or workflow:

1. Importing modules
2. Default Arguments
3. Instantiate a DAG
4. Tasks
5. Setting up Dependencies

For further details please refer to this amazing step-by-step [tutorial](https://www.applydatascience.com/airflow/writing-your-first-pipeline/) 
by Tuan Vu.

------------------------------------------------------------

## DAG Scheme on Batch Processing Cases

### Integrate Daily Search History
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=14o54tl6g3HznZ9h9pE_gnGvGTNdH0xr6">
</div><br />
DAG is scheduled to run everyday from 2021-03-10 to 2021-03-15,
picking data to be processed corresponding to the date.

### Integrate Transactional Data from Unified User Events
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1hd_DMJ52Yqiuwyv4xGV30hQvueHugO2m">
</div><br />
DAG is scheduled to run every 3 days, processing the table per 
3 (three) days of events, per DAG run.

## Implementation
Follow these steps to run the Batch Processing cases and trigger
Airflow Web UI to monitor the tasks:
1. Upload `jsonSchema.json` and `transformCSVtoJSON.js` in the
`support` folder to your `bucket-path`.
   
2. Upload `daily_search_result_dag.py` and `transactional_data_dag.py`
to `/dags` folder. This step will automatically trigger Airflow 
to run the tasks.
   
3. Go to Airflow Web UI and see your task status there


## Results
From the Airflow Web UI, you can see whether the task is successfully
run or not. You also can see the error message when the task failed
in the log url.
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1On5Ks3teU7zNofwUZvhzI3OthWqqWDXF">
<small> Failed Task </small>
</div><br />
