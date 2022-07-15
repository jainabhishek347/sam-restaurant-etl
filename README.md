# ETL Pipeline

The ETL Pipeline was created using AWS Lambda and deployed on Docker with scripts written in Python.
The ETL Pipeline takes raw data in form of CSV, transforms it, and systematically stores it into the PostgresSQL database.
Pandas library is used for transforming the data so that it can be appropriately stored in the database.
For this particular ETL Pipeline, the database contains four tables:
  1) Customer Table (customer_df): Stores information about the Customer including attributes like customer ID, Customer Name, etc.
  2) Product Table (product_df): Stores information about the Product including attributes like Product ID, Product Name, etc.
  3) Store Table (store_df): Stores information about the Store including attributes like Store Name, Product Name, etc.
  4) Basket Table (basket_df): Stores information about the basket created for an order having attributes like Product ID, Store ID, etc.
  
To enable the creation of multiple pipelines in the future, a GitHub action workflow is created.

![ETL Restaurant Diagram](https://user-images.githubusercontent.com/2360904/179221510-46e16a6c-7dcd-4a25-be20-c66031c3a519.jpg)

<h2> Process Flow </h2>
    <p>
      <ul>
      <li> The user downloads the sample file which contains the format for uploading a raw CSV file. </li>
     The user sends a CSV file containing raw data. The CSV file can be sent using the following three methods: email, FTP, and AWS S3. 
     <li> Subsequently, data will be sent to AWS S3 for storage. </li> 
     <li> Validation is carried out on the received file based on the following conditions:</li> 
     The number of columns in the received CSV file is equal to the  sample file given by the user</li>  
    The order and format of columns in the received CSV file should be identical to that of the sample file </li>       
     <li> For each set of data in a CSV file, AWS S3 will generate a log to find the number of entries and format of data. </li> 
      <li> If the aforementioned conditions are not met: the data will not be uploaded for further process and an error message will be sent   to the user by the AWS SQS and process will terminate.</li> 
      <li> If the conditions are met: AWS S3 will provide 4-5 events to manipulate data will be based on the requirements of the user (Examples of events include: edit, copy, delete, etc.) </li> 
      <li> Based on the selected event, a process on the AWS Lambda will be triggered and data will be processed and sent to the staging area.</li> 
      <li> Later, Processed data will be sent to AWS Redshift which will normalize the data which will be consumed by the Grafana for the recording process. If the  data insertion in AWS Redshift fails then a failure message will be sent by the  AWS SQS to the user and process will terminate. If data insertion is a sucess then a success message will be sent by the  AWS SQS to the user. </li> 
     <li> At the same time, AWS SQS will send data that will be consumed downstream. The Grafana will perform analytical operations based on user requirments. These operations include developing visualizations, alerts and dashboards. </li> 
     <li> During the entire process, we will introduce tables that will contain metadata, staging data, and finalized data. </li> 
      <li> A repository of all the scripts used for the process will be created. </li> 
    </p>
    
<h2> How to integrate GitHub action in your project </h2>

 ### 1. Pipeline Configuration 
To integrate GitHub actions in another project you have to copy the whole. Github folder to your repo, 
you can add test cases and edit the approver user for the production environment in the pipeline,

 ### 2. Deployment
Through AWS CLI, AWS is configured using credentials (AWS ACCESS KEY ID and Secret Key).
Subsequently, we log into Docker.
Finally, a repository is created to be deployed on docker.


## How ETL pipeline works
1. The raw CSV file is loaded into AWS S3.
2. The loaded data is taken from S3 and preprocessed using Pandas library in Python.
3. Based on triggers, AWS Lambda functions are invoked which store the data in the PostgresSQL database.
        


## Deploy the sample application

The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. It uses Docker to run your functions in an Amazon Linux environment that matches Lambda. It can also emulate your application's build environment and API.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* [Python 3 installed](https://www.python.org/downloads/)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

To build and deploy your application for the first time, run the following in your shell:

```bash
sam build --use-container
sam deploy --guided
```

