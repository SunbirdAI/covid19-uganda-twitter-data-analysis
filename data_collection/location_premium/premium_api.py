import requests
import json
import io
import time
import logging

from datetime import datetime
from data_collection.twitterv2.twitter_api import auth, create_headers, connect_to_endpoint
from data_collection.twitterv2.db_utils import save_records
from logging_utils import create_logger

PREMIUM_API_URL = "https://api.twitter.com/1.1/tweets/search/fullarchive/"
ACADEMIC_API_URL = "https://api.twitter.com/2/tweets/search/all"
TWITTER_API_URL = "https://api.twitter.com/2/"
logger = create_logger(__name__,
                       filename="ugandan_tweets.log",
                       logging_format="%(asctime)s: %(name)s: %(levelname)s: %(message)s",
                       logging_level=logging.DEBUG)

URL_CONFIG = {
    "premium": {
        "token_name": "TWITTER_PREMIUM_BEARER_TOKEN",
        "counts_url": f"{PREMIUM_API_URL}sbdev/counts.json",
    },
    "academic": {
        "token_name": "TWITTER_ACADEMIC_BEARER_TOKEN",
        "data_url": ACADEMIC_API_URL
    }
}


def create_json_data(query, from_date, to_date):
    return {
        "query": query,
        "fromDate": from_date,
        "toDate": to_date
    }


def setup_request_params(api_type, url_type="counts"):
    bearer_token = auth(URL_CONFIG[api_type]["token_name"])
    headers = create_headers(bearer_token)
    endpoint = URL_CONFIG[api_type][f"{url_type}_url"]
    return {
        "token": bearer_token,
        "headers": headers,
        "endpoint": endpoint
    }


def make_counts_request(query, from_date, to_date, bucket="day", api_type="academic"):
    params = setup_request_params(api_type, "counts")
    data = {
        "query": query,
        "fromDate": from_date,
        "toDate": to_date,
        "bucket": bucket
    }
    # print(json.dumps(data))
    results = []
    total_count = 0
    response = requests.post(params["endpoint"], json.dumps(data), headers=params["headers"]).json()
    num_of_requests = 1

    while True:
        if "results" in response:
            results.extend(response["results"])
            total_count += response["totalCount"]
        else:
            print(json.dumps(response, indent=2))
        if "next" in response:
            data["next"] = response["next"]
            response = requests.post(params["endpoint"], json.dumps(data), headers=params["headers"]).json()
            num_of_requests += 1
        else:
            break
    print(json.dumps(response, indent=2))
    print(f"Number of requests made: {num_of_requests}")
    with io.open('data/sample_tweets.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps({"results": results, "totalCount": total_count}, ensure_ascii=False, indent=5))


def make_data_request(url, api_type="academic"):
    bearer_token = auth(URL_CONFIG[api_type]["token_name"])
    headers = create_headers(bearer_token)
    results, status = connect_to_endpoint(url, headers)
    tweets = []
    tweets_so_far = 0
    request_made = 1
    while True:
        if "data" in results:
            tweets.extend(results["data"])
        if len(tweets) >= 1000:
            tweets_so_far += len(tweets)
            tweets_saved = save_records(tweets, "tweets", "ugandan")
            if tweets_saved == 0:
                logger.error("No tweets saved. Found duplicates")
            else:
                logger.info(f"Saved {tweets_saved} new tweets. Total tweets collected so far {tweets_so_far}. "
                            f"Number of requests: {request_made}")
            tweets = []
            print(f"Progress -> tweets collected: {tweets_so_far} requests made: {request_made}")
        if "meta" in results and "next_token" in results["meta"]:
            new_url = url + f"&next_token={results['meta']['next_token']}"
            results, status = connect_to_endpoint(new_url, headers)
            if status == 200:
                request_made += 1
            elif status == 429:
                while status == 429:
                    logger.info("Rate limit exceeded.")
                    logger.info(f"Requests made so far: {request_made}")
                    logger.info(f"Tweets collected so far: {tweets_so_far}")
                    logger.info("Waiting for 10 minutes")
                    print(f"Progress (rate limit exceeded) -> tweets collected: {tweets_so_far}"
                          f" requests made: {request_made}")
                    time.sleep(600)
                    logger.info("Making request again...")
                    results, status = connect_to_endpoint(new_url, headers)
                    if status == 200:
                        request_made += 1
            else:
                logger.info(f"Error {status}: {results}")
                break
        else:
            break
    logger.info("\n\nFinished making request")
    logger.info(f"Tweets collected: {tweets_so_far}")
    logger.info(f"Total requests made: {request_made}")


def create_tweets_url(query, start_time, end_time, max_results=500, time_type="recent"):
    tweet_fields = "tweet.fields=id,author_id,conversation_id,text,created_at,in_reply_to_user_id,geo,public_metrics" \
                   ",source,referenced_tweets&expansions=author_id,in_reply_to_user_id,referenced_tweets.id" \
                   "&user.fields=name,username"

    url = TWITTER_API_URL + f"tweets/search/{time_type}?query={query}" \
                            f"&max_results={max_results}&start_time={start_time}&end_time={end_time}&{tweet_fields}"
    return url


if __name__ == '__main__':
    # For premium and counts api
    # _from_date = datetime(2020, 3, 1).strftime("%Y%m%d%H%M")
    # _to_date = datetime(2021, 3, 2).strftime("%Y%m%d%H%M")

    # For academic twitter
    _from_date = datetime(2020, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ")
    _to_date = datetime(2021, 3, 1).strftime("%Y-%m-%dT%H:%M:%SZ")

    # make_counts_request("place_country:UG", _from_date, _to_date, api_type="academic")
    # make_data_request("place_country:UG since", _from_date, _to_date)
    _url = create_tweets_url("place_country:UG", start_time=_from_date,
                             end_time=_to_date, max_results=500, time_type="all")
    make_data_request(_url)
