# Twitter-ETL-airflow

## Table of Contents:
1. Objective
2. Architecture
3. Modules

### 1. Objective:
    Extract the data from the twitter api, clean and transform it in the format that can be used for analysis and then load it to the AWS Redshift warehouse using airflow scheduler.

### 2. Architecture:
     Project uses following components:
     
![twitter drawio](https://user-images.githubusercontent.com/16570874/148721784-850b7ab7-93e2-464f-aaa8-741561fb950a.png)

Firstly, data is extracted from twitter api at the interval of an hour within predefined startdate and enddate. 100 data is extracted per hit and are stored in a json format in local storage.
Secondly, extracted data from api is cleaned, formatted and only useful fetures are saved into csv file in local computer.
Lastly, stored csv file is loaded into AWS Redshift for further analysis.
Airflow is used here as a job scheduler to extract, transform and load the data.
