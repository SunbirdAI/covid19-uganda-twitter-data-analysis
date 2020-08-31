import requests
import os
import json
from dotenv import load_dotenv
from data_collection.twitterv2.db_utils import save_records


TWITTER_API_URL = "https://api.twitter.com/2/"


def auth():
    """
    Get the bearer token for authentication
    @:return bearer token
    """
    load_dotenv()
    return os.getenv("TWITTER_BEARER_TOKEN")


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_users_url(usernames=None, ids=None):
    by = "usernames" if usernames is not None else "ids"
    list_to_use = usernames if usernames is not None else ids
    user_fields = ["id", "name", "username", "created_at", "description", "location", "public_metrics", "verified"]
    tweet_fields = ["author_id", "created_at"]
    url = TWITTER_API_URL + "users/by?{}={}&user.fields={}&expansions=pinned_tweet_id&tweet.fields={}" \
        .format(by, ",".join(list_to_use), ",".join(user_fields), ",".join(tweet_fields))
    return url


def filter_ugandan_users(users):
    """
    Filters the list of users to find only those whose location contains the word Uganda or Kampala
    :param users: list of users
    :return: list of Ugandan users as described above
    """
    ugandan_users = []
    for user in users:
        if "location" in user and ("Kampala" in user["location"] or "Uganda" in user["location"]):
            ugandan_users.append(user)
    return ugandan_users


def create_query(users=None, conversation_id=None, entities=None):
    """
    Creates the query string depending on the parameters given.
    :param users: list of users whose tweets to get.
    :param conversation_id: the id of the conversation from which to get tweets
    :param entities: a list of entities (such as hashtags or annotations)
    :return: the query string
    """
    if users is not None:
        return " OR ".join("from:{}".format(user) for user in users) + " -is:retweet"
    if conversation_id is not None:
        return "conversation_id:{} -is:retweet".format(conversation_id)
    if entities is not None:
        return " OR ".join("entity:{}".format(entity) for entity in entities)
    return ""


def create_tweets_url(query):
    tweet_fields = "tweet.fields=id,author_id,conversation_id,text,geo,public_metrics,source"
    url = TWITTER_API_URL + "tweets/search/recent?query={}&{}".format(query, tweet_fields)
    return url


def fetch_records(url, record_type="tweets"):
    bearer_token = auth()
    headers = create_headers(bearer_token)

    records = []
    conversation_ids = set()
    user_ids = set()
    json_response = connect_to_endpoint(url, headers)
    records_retrieved = 0
    while True:
        records.extend(json_response["data"])
        if len(records) >= 100:
            records_retrieved += len(records)
            if record_type == "tweets":
                conversation_ids.union(extract_conversation_ids(records))
                user_ids.union(extract_author_ids(records))
                save_records(records, record_type)
            else:
                ugandans = filter_ugandan_users(records)
                save_records(ugandans, record_type)
            records = []
        if "meta" in json_response and "next_token" in json_response["meta"]:
            new_url = url + "&next_token={}".format(json_response["meta"]["next_token"])
            json_response = connect_to_endpoint(new_url, headers)
        else:
            # TODO: Log number of requests made, and number of records collected (and type of record)
            print("Records collected: {}".format(records_retrieved))
            break

    if len(records) > 0:
        records_retrieved += len(records)
        if record_type == "tweets":
            conversation_ids.union(extract_conversation_ids(records))
            user_ids.union(extract_author_ids(records))
            save_records(records, record_type)
        else:
            ugandans = filter_ugandan_users(records)
            save_records(ugandans, record_type)
    return records_retrieved, conversation_ids, user_ids


def extract_conversation_ids(tweets):
    return set([tweet["conversation_id"] for tweet in tweets])


def extract_author_ids(tweets):
    return set([tweet["author_id"] for tweet in tweets])


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        print("No more results: Status code: {} Response: {}".format(response.status_code, response.text))
        return {"data": [], "meta": {}}
    return response.json()
