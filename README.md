Novum Airflow
===


Build
--
```
$ docker build . -t airflow
```
Make sure you have been given the `secrets/` folder and that it has been placed at the repo root level. Otherwise nothing
will work. alberto@novuminsights.com will be happy to oblige.

Deployment
--

```
$ docker run -D -p 8080:8080 airflow
```
Develop
--
Airflow is configured to use a `LocalExecutor` and is back by a PostgreSQL RDS instance. 
Webserver authentication is basic password stored in the database and salted using `flask_bcrypt`.
The salt key is stored as the environmental variable `AIRFLOW__CORE__FERNET_KEY`.
Supplying no key or the wrong key will lead to connection issues as passwords
supplied will be deemed incorrect.  

Please note that environmental variables supersede anything in `airflow.cfg`. 

All dependencies needed to make airflow work are contained in `Dockerfile`. 
Any additional Python packages need for running tasks should be added to `requirements.txt`. 


Architecture
===
The ETL process is designed around two principles:
1. Raw data should be stored in the event it needs to be reprocessed later but 
is no longer available from the original source.

2. Tasks should be decoupled -- to the extent it is practical.  

DAGs
--
Directed Acyclic Graphs -- Airflow DAGs are how you specify a collection of tasks
and how the depend on one another (see Airflow docs). Nothing special is done with
these -- they are just Airflow dags. 

Tasks
--
Airflow tasks. DAGs are a collection of such tasks.
We are mostly using the `PythonOperator`. 
This project refers to Python callable as a "task wrapper".

Task Wrappers
--
Task Wrappers are used to separate Airflow aware code from other code. 
Currently that means dealing with Airflow's XCOM system and unpacking the `kwargs` from
that the `PythonOperator` passes down to the callable.

Task Wrappers are the callable used by tasks and wrap "Runners" .

Runners
--
Runners encapsulate a given routine.

All current runners simply call an extractor or transform, 
loads the output into one of our data stores (S3, DB) in an appropriate format,
and returns any information that is needed for downstream tasks that would be 
otherwise inaccessible (i.e. needed by XCOMS).

Paths
--
Paths are currently just templates to locations on S3 or DB tables. 

This information needs to be separate from the extractors. Imagine you extract
some raw data from a REST api and store it on S3. Later you need to process that data 
via a transformer. The Transformer needs to know where the extractor put it, but
you probably don't want to import `requests` and everything else your extractor
depends upon just to get an S3 path.

Extractors
--
Extractors extract raw, disgusting, unpredictable data from the cold, cruel, outside world.
They do not clean, filter, or load data. Extractors should do as little as possible and
be as simple as possible to avoid mistakes and omissions. Issues in other, latter steps can be
corrected (well not loading the extracts, but everything after that).

Transformers
--
Transformers, where they exist, perform complex tasks of cleaning and transforming data provided by the
extractors into a format which is friendly to store. Whilst more complicated transformer tasks are
encapsulated in their own methods, simpler routines are placed directly into the runner to aid 
readability and reduce complexity.

Deploy to Production
==
As mentioned, the Airflow instance is encapsulated in a docker image. We use AWS Docker Container Registry
and AWS CodeDeploy to deploy and run it on AWS EC2 instance.

Please follow these steps on the terminal (cd into root dir of project) to deploy. A Bitbucket Pipeline is also provided for continuous
deployment.

```bash
docker build --no-cache  -t 882924714353.dkr.ecr.eu-west-1.amazonaws.com/warehouse-airflow:master .

aws ecr get-login-password \
    --region eu-west-1 \
| docker login \
    --username AWS \
    --password-stdin 882924714353.dkr.ecr.eu-west-1.amazonaws.com
    
docker push 882924714353.dkr.ecr.eu-west-1.amazonaws.com/warehouse-airflow:master

zip -r build/deploy_novum_airflow.zip scripts appspec.yml

aws --region eu-west-1 s3 cp build/deploy_novum_airflow.zip s3://codedeploy-novumdpairflow/

aws --region eu-west-1 deploy create-deployment --application-name novum_dp_airflow \
            --deployment-config-name CodeDeployDefault.OneAtATime \
            --deployment-group-name novum_dp_airflow \
            --s3-location bucket=codedeploy-novumdpairflow,bundleType=zip,key=deploy_novum_airflow.zip
```

CodeDeploy and the Bitbucket Pipeline call bash scripts in the `scripts/` folder, which you are invited to explore.