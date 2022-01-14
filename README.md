# Twitter-ETL-airflow

## Table of Contents
1. Objective
2. Architecture
3. Modules

### Objective
Extract the data from the twitter api, clean and transform it in the format that can be used for analysis and then load it to the AWS Redshift warehouse using airflow scheduler.

### Architecture
Project uses following components:
     
![twitter drawio](https://user-images.githubusercontent.com/16570874/148721784-850b7ab7-93e2-464f-aaa8-741561fb950a.png)

Firstly, data is extracted from twitter api at the interval of an hour within predefined startdate and enddate. 100 data is extracted per hit and are stored in a json format in local storage.

Secondly, extracted data from api is cleaned, formatted and only useful fetures are saved into csv file in local computer.

Lastly, stored csv file is loaded into AWS Redshift for further analysis.

Airflow is used here as a job scheduler to extract, transform and load the data.

### Modules
a. Extraction: Twitter API is used to fetch the data. The credentials for Twitter API is kept inside airflow variables which can be fetched as below:
    
 BEARER_TOKEN=Variable.get("twitter-bearer-token")

Twitter API:
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
            
 The extracted response is in the form of Json file which is stored in local computer.
            
