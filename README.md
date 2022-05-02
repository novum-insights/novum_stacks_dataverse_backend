Stacks Dataverse Backend (Airflow)
===

This repo contains the [Apache Airflow](https://airflow.apache.org/) implementation of the
[Stacks Dataverse](https://stacks.novuminsights.com/) backend, developed by [Novum Insights](https://www.novuminsights.com/).

This repo is not intended to be cloned and run as-is. Instead it is a blueprint upon which to set up an environment (see below)
and run the routines to fetch data from various Stacks sources. For this reason, forking is strongly suggested.

Prerequisites
--
* Docker
* Apache Airflow and a supporting Postgres database (and familiarity with both)
  
* A *Stacks API/Stacks Blockchain/Stacks Database* setup. Instructions [here](https://docs.hiro.so/get-started/running-api-node) 
  on how to set this up. (Warning: high-performance server needed)
* An AWS account is **highly recommended** as the container is deployed on EC2 via CodeDeploy, post-processed
data is stored in RDS DB as well as an S3 bucket which is hooked to CloudFront. The point above also runs on EC2.
  
* Python 3.7 or higher
  
* BitBucket is recommended if a CI/CD pipeline is required, although the `bitbucket-pipelines.yml` file can be
  easily modified for Github Actions or any other such system. Please fork the repo for such purposes.
  
* A Cryptocompare API key. Get one [here](https://min-api.cryptocompare.com/pricing) (premium recommended)
  
* A LunarCrush API key is recommended. Get one [here](https://legacy.lunarcrush.com/).
* Coffee and Patience, as many variables inside the code are placeholders to be modified with own parameters.

Initial Setup
--

*(Feedback on the [lack of] clarity of this section? Please email alberto@novuminsights.com)*

1. Set up the *Stacks API/Stacks Blockchain/Stacks Database* component using the instructions provided above. Ec2 is recommended.
2. Set up an empty Postgres Database. This will be used for Airflow task management. Also set up a Fernet key (see `scripts/entrypoint.sh` for more details).
   
3. Add an `env.list` file to the root directory with the following contents you created in step 2:
```
POSTGRES_PASSWORD=XXXXXXX
POSTGRES_HOST=XXXXXXXX
AIRFLOW__CORE__FERNET_KEY=XXXXXXXX
```
4. With the help of the above components, set up an Airflow instance and its root user. See [this tutorial](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).
Make sure it uses the resources set up in step 2. You will find that the vast majority of the tasks will not work: the next steps
   will fix that.
   
5. Substitute the relevant entries and placeholders in `airflow.cfg`, the `scripts` folder and the `Dockerfile` with what has been set up so far.

6. Set up AWS things: An Ec2 to host the Airflow Docker image, S3 for both the Stacks data and the CodeDeploy, Codedeploy, CloudFront (hooked to the S3 Stacks data bucket), RDS for more Stacks data.
   (It is not in the scope of this readme to provide details on how to set up each). AWS is not compulsory but is highly recommended. 
7. Run through the code and substitute more placeholders with what has been set up so far.
   
8. Create a `secrets/` folder in which you should put the RSA private key needed to SSH into the EC2 instance on which your Airflow docker container will run.
   We have called this file `id_rsa_ec2`.
9. You should now be ready to deploy your Docker container with the Airflow instance. Please refer to the "Deploy" section below.
10. Assuming step 9 was successful, you have now entered the final parts of the setup. Congratulations! 
11. Log in to the Airflow instance with the user you created in step 4 and set up `Variables`. Identify in 
    the code which ones are needed and use the Airflow site to add them.
    
12. Create another database which will contain historical STX price data. Call the table `stx_historical_prices` with two columns: `unix_timestamp` (int) and `price_usd` (float).
13. Set up `Connections`. These will be to two databases. 1- The Stacks database created in step 1, and 2-The database (warehouse) created in step 12.
14. Set up a `Pool` called `stacks-pool`.

Build
--
```
$ docker build . -t airflow
```
Make sure you have been given the `secrets/` folder (or you have created it yourself) and that it has been placed at the repo root level. Also remember `env.list`.

Run locally
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
docker build --no-cache  -t YOUR_ECR_IMAGE_ID_HERE:master .

aws ecr get-login-password \
    --region YOUR_AWS_REGION \
| docker login \
    --username YOUR_AWS_USERNAME \
    --password-stdin YOUR_ECR_IMAGE_ID_HERE
    
docker push YOUR_ECR_IMAGE_ID_HERE:master

zip -r build/deploy_airflow.zip scripts appspec.yml

aws --region YOUR_AWS_REGION s3 cp build/deploy_airflow.zip s3://YOUR_CODEDEPLOY_BUCKET/

aws --region YOUR_AWS_REGION deploy create-deployment --application-name YOUR_CODEDEPLOY_APPLICATION_NAME \
            --deployment-config-name CodeDeployDefault.OneAtATime \
            --deployment-group-name YOUR_CODEDEPLOY_GROUP_NAME \
            --s3-location bucket=YOUR_CODEDEPLOY_BUCKET,bundleType=zip,key=deploy_novum_airflow.zip
```

CodeDeploy and the Bitbucket Pipeline call bash scripts in the `scripts/` folder, which you are invited to explore.