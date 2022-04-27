#!/usr/bin/env bash
set -e
AWS="aws --profile <profile> --region eu-west-1"

if [ ! -d build ]
then
  mkdir build
fi

zip -r build/deploy_airflow.zip scripts appspec.yml
$AWS s3 cp build/deploy_airflow.zip s3://YOUR_CODEDEPLOY_BUCKET/

deployment=$(${AWS} deploy create-deployment --application-name YOUR_CODEDEPLOY_APPLICATION_NAME \
            --deployment-config-name CodeDeployDefault.OneAtATime \
            --deployment-group-name YOUR_CODEDEPLOY_GROUP_NAME \
            --s3-location bucket=YOUR_CODEDEPLOY_BUCKET,bundleType=zip,key=deploy_airflow.zip)

$AWS deploy wait deployment-successful  --cli-input-json "$deployment"