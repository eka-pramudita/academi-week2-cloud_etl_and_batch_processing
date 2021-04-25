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
I used one of the most popular cloud service, the 
[Google Cloud Platform (GCP)](https://cloud.google.com/).

<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1IwiVT8_2bX7v-P-Zk5R6czQyXvKxoRzt">
</div><br />

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
   Set up variables we will be using in Variables. Set: 
   1. `bucket_path` (path of your created bucket)
   2. `project_id` (your project id)
4. Google Cloud Storage. Create a bucket in Google Cloud Storage to store data sources and additional files
   needed. For complete steps please refer [this page](https://cloud.google.com/composer/docs/how-to/using/using-dataflow-template-operator)
   
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1UjoUTatco77GbYR1Jb49g7nuD_lG0dQG">
<small>Google Cloud Composer Environment Monitoring Tab</small>
</div><br />

## Apache Airflow
Apache Airflow is a platform to programmatically (using Python) author, schedule, and monitor workflows.
Workflows are implemented as directed acyclic graphs (DAGs) of tasks. With Airflow, you can
schedule your tasks and specify dependencies among them. Pipelines also generated to monitor
tasks status and troubleshoot problem if needed.

<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1C4eqGhJLqf3-iYfVt0NLz67gOvBluYkd">
<small> Airflow Web UI </small>
</div><br />

### Writing DAGs
There are 5 steps you need to remember to write an Airflow DAG or workflow:

1. Importing modules
2. Default Arguments
3. Instantiate a DAG
4. Tasks
5. Setting up Dependencies

For further details please refer to this amazing step-by-step [tutorial](https://www.applydatascience.com/airflow/writing-your-first-pipeline/) 
by Tuan Vu.

## Batch Processing Cases

### Integrate Daily Search History

Running Interval: 2021-03-10 until 2021-03-15

Schedule: Daily

**Tasks:**
1. Load .csv files into BigQuery (BQ) table. BQ Schema is same as .csv files.
2. Convert fields into correct format.
3. Get the most searched word and store to BQ table.

**Running steps:**
1. Upload dag file `daily_search_results_dag.py` to `dags/` folder of your environment.
2. Check the task status on Airflow Web UI

**Result:**

<div align="center">
  <img src="https://drive.google.com/uc?export=view&id=13bnFAqdmZpIuEvju8tl6JPmrMWF4Kac0" width="200" />
  <img src="https://drive.google.com/uc?export=view&id=17hIhTOkQbg7UQTJttiVIyQ3kHGfc9nam" width="200" /> 
  <img src="https://drive.google.com/uc?export=view&id=1CnV3AS85ihzwPJrtcbdgTJdPRNsBk2ll" width="200" />
</div>

**Using Dataflow:**

Before uploading dag file, upload `transformCSVtoJSON.js` and `jsonSchema.json` files to
your `bucket_path`. However, when I applied the steps, not all dataflow job ran successfully.
Since it is unstable, then I didn't use dataflow for this task.

<div align="center">
  <img src="https://drive.google.com/uc?export=view&id=1uUfqALvGZc_SbU-TIwTRBxMA_6RQxCa8" width="200"/> 
  <img src="https://drive.google.com/uc?export=view&id=1e4t6KZL2N9I6O1A9jAptJeErQCStFcQs" width="200"/> 
</div>


### Integrate Transactional Data from Unified User Events

Running Interval: 2021-03-21 until 2021-03-27

Schedule: Once in 3 days

**Tasks:**
1. Get events data from external BQ table. Preprocess then store it into own BQ dataset as a raw data. (This task is not included in Airflow)
2. Query the raw data for 3 days of events to get values needed then store it into the transactions table.

**Running steps:**
1. Install requirements needed by running this command in your virtual environment:
   ```
   pip install -r requirements.txt
   ```
2. Run `store_events_to_bigquery.py` to get, process and put events data to own BQ:
   ```
   python store_events_to_bigquery.py
   ```
3. Upload daf file `transactions_data_from_events_dag.py` to `dags/` folder of your environment.
4. Check the task status on Airflow Web UI

**Results:**
<div align="center">
  <img src="https://drive.google.com/uc?export=view&id=1t_fPtd2Y-FfUCKF9W6urnI6g4jalC0ZX" width="200" />
  <img src="https://drive.google.com/uc?export=view&id=14NRjSVUzkI5NQ4G92GUUiLppm86VcTL6" width="200" /> 
</div>

**JSON type data handling on BQ:**

When you put data in BQ, JSON type data will be automatically parsed so it is much easier
to get the value of it.
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1l5Heitmdl8rD-NQeaK0UPfEeiw2rG9kV">
</div><br />

## Conclusion
Cloud technology enables an easier way to do batch processing tasks rather than using on-premise hardware.
Google Cloud Platform specifically provides a great environment to perform ETL jobs, data pipelining and tasks scheduling
with all of their services and also supported by Apache Airflow. 
However, the service stability is somewhat needing attention especially when using Dataflow.