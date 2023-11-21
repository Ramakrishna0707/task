# TAIYO.AI task #

## BigData Task
1. Create a Scalable data pipeline using any one frameworka. Pyspark,
b. Scala Spark
c. logstash
2. Data flow pipelines should have plugable transformation functions. And write sample
transformations.
a. Group By some Measure
b. Join some other Data Pipeline or static datasets
3. Read/Write should be considering connector based design to make data flow from/to S3,
GCP, Files, Hadoop, SQL DB or file storage (any one sample good)
4. Read/Write Data in multiple formats Json, Parquet, gZip
5. Publish Data Pipeline as application. Build and Deploy package definition and
development. Deployment package should be separate from the source code.
6. Write CICD Notes and Readme file to simulate pipeline in test environment

## Project Structure
```bash

├── src/                  # Source code directory
│   ├── pipeline.py       # Main Spark script
│   ├── transformations.py # Module for data transformations
├── requirements.txt      # List of project dependencies
├── data/                 # Directory for storing data files
├── README.md             # Project documentation
├── setup.py              # Script for packaging and distribution
```

## Overview 

This project is Spark-based data processing pipeline. The main Script 'pipeline.py', and transformations script 'transformations.py' module encapsulates the key data manipulation functions

I created an AWS account and then created an S3 bucket called "ram-tast" and an IAM user for programmatic access. For this user, I generated an AWS access key ID and secret access key. I uploaded some feature data and store data into two objects within the "ram-tast" S3 bucket. Now there are two objects containing my feature and store data residing in my "ram-tast" S3 bucket in AWS that I can access programmatically using the IAM user credentials.

## Prerequisites
- AWS Credentials

aws acess key id = ""
aws secret acess key = ""
 - Python
 - Pyspark
 - boto3 

 ## SETTING UP THE ENVIRONMENT

VS Code
```bash
conda create  -p venv python -y
```
activate the vnev
```bash
conda activate venv/ 

```
- ### Installation:
   Install the requireed python dependencies listed in "requirements.txt". You can do this by running the following command in your project directory.
   
```bash
   pip install -r requirements.txt

```

- To setup the requirements file, Run this command will execute the all the required libraries
```bash
setup.py
``` 

## USAGE

```bash
python pipeline.py
python transformations.py 
```

