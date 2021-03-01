import requests
import json
import io

from datetime import datetime
from data_collection.twitterv2.twitter_api import auth, create_headers

PREMIUM_API_URL = "https://api.twitter.com/1.1/tweets/search/fullarchive/"


def create_json_data(query, from_date, to_date):
    return {
        "query": query,
        "fromDate": from_date,
        "toDate": to_date
    }


def make_counts_request(query, from_date, to_date, bucket="day"):
    bearer_token = auth("TWITTER_PREMIUM_BEARER_TOKEN")
    headers = create_headers(bearer_token)
    endpoint = f"{PREMIUM_API_URL}sbdev/ug_tweets_counts.json"
    data = {
        "query": query,
        "fromDate": from_date,
        "toDate": to_date,
        "bucket": bucket
    }
    # print(json.dumps(data))
    results = []
    total_count = 0
    response = requests.post(endpoint, json.dumps(data), headers=headers).json()

    while True:
        if "results" in response:
            results.extend(response["results"])
            total_count = response["totalCount"]
        else:
            print(json.dumps(response, indent=2))
        if "next" in response:
            data["next"] = response["next"]
            response = requests.post(endpoint, json.dumps(data), headers=headers).json()
        else:
            break
    print(json.dumps(response, indent=2))
    with io.open('data/ug_tweets_counts.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps({"results": results, "totalCount": total_count}, ensure_ascii=False, indent=5))


if __name__ == '__main__':
    _from_date = datetime(2020, 3, 1).strftime("%Y%m%d%H%M")
    _to_date = datetime(2021, 2, 1).strftime("%Y%m%d%H%M")
    make_counts_request("place_country:UG", _from_date, _to_date)
