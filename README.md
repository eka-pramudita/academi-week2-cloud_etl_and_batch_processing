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
[Google Cloud Platform](https://cloud.google.com/).

## Google Cloud Platform (GCP) Setup
There are some setup on tools that needed to be done before using
GCP for ETL. GCP provides $300 free trial credit for 3 months so
after applying the free trial, things that we need to set up are:

1. Service Accounts. Make sure that Service Account used has Owner role
   to enable all Google Services.
2. Google Cloud Composer. This is a fully managed workflow orchestration service
   built in popular Apache Airflow open source project and operated using the Python programming language.
   Create an environment here by using the Service Account that has Owner role.
   For complete steps please refer [this page](https://cloud.google.com/composer/docs/how-to/managing/creating).
3. Create a bucket in Google Cloud Storage to store additional files
   needed. For complete steps please refer [this page](https://cloud.google.com/composer/docs/how-to/using/using-dataflow-template-operator)
4. After environment created, go to Environment Configuration then
   click the Airflow web UI. Set up variables used during ETL process
   in Admin -> Variables. Set: 
   1. `bucket_path` (path of your created bucket)
   2. `project_id` (your project id)
   3. `gce_region` (region of you environment e.g us-east4)
   4. `gce_zone` (region of you environment e.g us-east4-c)
   
## DAG


