import json
import io
from dateutil import parser
from datetime import datetime
from constants import DATA_FOLDER


def retrieve_file_tweets(filename):
    with io.open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
        return data['tweets']


def get_geo_ids(tweets):
    geo_ids = set()
    for tweet in tweets:
        if tweet is None:
            break
        if "geo" in tweet:
            geo_ids.add(tweet['geo']['place_id'])
    return geo_ids


def all_geo_ids():
    geo_ids = set()
    for i in range(1, 99):
        tweets = retrieve_file_tweets(DATA_FOLDER.joinpath(f"ug_tweets_{i}.json)"))
        geo_ids = geo_ids.union(get_geo_ids(tweets))
        if i % 10 == 0:
            print(f"Unique geo ids so far {i}: {len(geo_ids)}")
    return geo_ids


def get_all_tweets(num_of_files=98):
    tweets = []
    for i in range(1, num_of_files + 1):
        tweets.extend(retrieve_file_tweets(DATA_FOLDER.joinpath(f"ug_tweets_{i}.json")))
    return tweets


def divide_tweets_into_months(tweets):
    monthly_tweets = dict()

    for tweet in tweets:
        if tweet is None:
            break
        else:
            created_time: datetime = parser.parse(tweet['created_at']).replace(tzinfo=None)
            key = f"{created_time.year}-{created_time.month}"
            if key in monthly_tweets:
                monthly_tweets[key].append(tweet)
            else:
                monthly_tweets[key] = []
                monthly_tweets[key].append(tweet)

    for key in monthly_tweets.keys():
        print(f"{key}: {len(monthly_tweets[key])}")
    return monthly_tweets


if __name__ == '__main__':
    # tweets_ = retrieve_file_tweets('data/ug_tweets_1.json')
    # geo_ids_ = all_geo_ids()
    # print(f"Unique geo ids: {len(geo_ids_)}")
    tweets_ = [tweet_ for tweet_ in get_all_tweets() if tweet_ is not None]
    print(len(tweets_))
    divide_tweets_into_months(tweets_)
