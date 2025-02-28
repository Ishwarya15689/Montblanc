<h1>Introduction</h1>

This repository contains the application code to deploy montblanc_veeva_importer in ECS Fargate using MWAA as an orchestration and scheduling tool. It uses the configuration of 8GB RAM and 2vCPUs memory configuration in task definiton for running full load and 5GB RAM and 2vCPUs memory for delta load. There are totally 4 datasets - enrollment metrics, milestone, study, study country which is run in a single container and sequence of dataset is handled within the code.

<h2>Prerequisite</h2>

- Docker should be installed.
- For building the image, it also requires montblanc_utils from this directory, common_utils folder from the shared library directory(integrations/shared-lib/common-utils/src/) in the working directory.

<h2>Build</h2>


To build the docker images, you just need to run the following command:

> *docker build -t <image_name> .*

<h2>Push to ECR</h2>

To push the image to ECR, follow :

- *aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com*
- *docker tag montblanc_veeva_importer:latest <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/ngctms-<ENV>-ec1-montblanc-veeva-importer-ecr:latest*
- *docker push <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/ngctms-<ENV>-ec1-montblanc-veeva-importer-ecr:latest*
- *docker tag montblanc_veeva_importer:latest <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/ngctms-<ENV>-ec1-montblanc-veeva-importer-ecr:1.0.0*
- *docker push <ACCOUNT_NUMBER>.dkr.ecr.eu-central-1.amazonaws.com/ngctms-<ENV>-ec1-montblanc-veeva-importer-ecr:1.0.0*

The image will be pushed to ECR with the version tag and the latest tag pointing to the latest version.

<h2>Troubleshooting</h2>

For any issue during docker run, validate the logs for the error and take action accordingly.

If there is issue with the space getting filled up in EC2 while testing, delete all the untagged images using :

> *docker rmi -f $(docker images | grep "none" | awk '{print $3}')*