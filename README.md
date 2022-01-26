# Twitter ETL Airflow
> Description

## Installation
   Clone this repo and go the directory in the terminal where you have project repo.
   In terminal, run:
   ```bash
   $ echo -e "AIRFLOW_UID=$(id -u)" > .env
   $ docker-compose up airflow-init
   $ docker-compose up 
   ```
   
   
## Table of Contents
1. Objective
2. Architecture
3. Modules

## Objective
Extract the data from the twitter api, clean and transform it in the format that can be used for analysis and then load it to the AWS Redshift warehouse using airflow scheduler.

## Architecture
Project uses following components:
     
![twitter drawio](https://user-images.githubusercontent.com/16570874/148721784-850b7ab7-93e2-464f-aaa8-741561fb950a.png)

Firstly, data is extracted from twitter api at the interval of an hour within predefined startdate and enddate. 100 data is extracted per hit and are stored in a json format in local storage.

Secondly, extracted data from api is cleaned, formatted and only useful fetures are saved into csv file in local computer.

Lastly, stored csv file is loaded into AWS Redshift for further analysis.

Airflow is used here as a job scheduler to extract, transform and load the data.

## Modules
### a. Extract: 
 Twitter API is used to fetch the data. In order to get the access of Twitter API, we need to get the credentials.
 Please click [here](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api) 
 to signup twitter api developer account.
 These credentials for Twitter API is kept inside airflow variables which can be fetched as below:

  ```
  BEARER_TOKEN=Variable.get("twitter-bearer-token")
  ```
  Twitter API:
  ```python
  search_url="https://api.twitter.com/2/tweets/search/recent"
  query="covid"  
  tweet_fields = "tweet.fields=text,created_at,referenced_tweets"
  user_fields = "user.fields=username,name,verified,location"
  start = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S%z')
  end = start + datetime.timedelta(hours=1)
  start_time = start.strftime('%Y-%m-%dT%H:%M:%SZ')
  end_time = end.strftime('%Y-%m-%dT%H:%M:%SZ')
  url = "{}?query={}&{}&{}&expansions=author_id,referenced_tweets.id&max_results=10&start_time={}&end_time={}".format(
      search_url, query, tweet_fields, user_fields, start_time, end_time
  )
  ```

  The extracted response is in the form of Json file which is stored in local computer.

  ### b. Transform:
 The extracted JSON file is cleaned and converted into dataframe for further exploratory analysis and only data with 5 columns                                     [author_id,created_at,id,original_text,location] were converted to csv file and is saved to local computer.

  ### b. Load:
 In order to load data into AWS Redshift, first we need to create AWS developer account. Click [here] (https://docs.aws.amazon.com/ses/latest/dg/setting-up.html) to create an account.
 In Redshift Create a database tweet and use below query to create the table:
 ```sql
 
 CREATE TABLE tweet
 (
      author_id        varchar(80),
      created_at       timestamptz,
      id     		     varchar(80),
      original_text    char(1000),
      location	        varchar(100)
 ) ;
  ```
  Then use Redshift connection id in your python code to connect python code to Redshift and send the csv file into Redshift in sql format.
          
  ### d. Airflow:
 Put bearer-token, that you got from Twitter API under airflow->Admin->Variables

 Add connection of airflow to Redshift from airflow -> Admin -> Connections

 User https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/redshift.html for more details.

 On success, Dagger graph should look like this:

![Screenshot from 2022-01-15 01-42-19](https://user-images.githubusercontent.com/16570874/149612222-829c343e-17cf-45e8-b427-5cb683aec961.png)

 Tree should look like this:

![Screenshot from 2022-01-15 01-42-25](https://user-images.githubusercontent.com/16570874/149612226-c54dd1c6-b8f5-4427-86b8-205a5e14ff8a.png)


## Authors

* [Minu Sarraf](mailto:minusarraf96@gmail.com)


## License

This source code is released under the [MIT License](LICENSE)          
          
          

