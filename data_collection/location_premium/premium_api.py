import requests
import json
import io

from datetime import datetime
from data_collection.twitterv2.twitter_api import auth, create_headers, connect_to_endpoint

PREMIUM_API_URL = "https://api.twitter.com/1.1/tweets/search/fullarchive/"
ACADEMIC_API_URL = "https://api.twitter.com/2/tweets/search/all"
TWITTER_API_URL = "https://api.twitter.com/2/"

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
    print(f"Status code: {status}")
    print(json.dumps(results, indent=2))


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
    _to_date = datetime(2020, 3, 2).strftime("%Y-%m-%dT%H:%M:%SZ")

    # make_counts_request("place_country:UG", _from_date, _to_date, api_type="academic")
    # make_data_request("place_country:UG since", _from_date, _to_date)
    _url = create_tweets_url("place_country:UG", start_time=_from_date,
                             end_time=_to_date, max_results=10, time_type="all")
    make_data_request(_url)
