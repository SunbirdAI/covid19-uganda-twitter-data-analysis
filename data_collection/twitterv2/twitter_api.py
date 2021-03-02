import requests
import os
from dotenv import load_dotenv
from data_collection.twitterv2.db_utils import save_records


TWITTER_API_URL = "https://api.twitter.com/2/"


def auth(token_type):
    """
    Get the bearer token for authentication
    @:return bearer token
    """
    load_dotenv()
    return os.getenv(token_type)


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_users_url(usernames=None, ids=None):
    by = "/by?usernames" if usernames is not None else "?ids"
    list_to_use = usernames if usernames is not None else ids
    user_fields = ["id", "name", "username", "created_at", "description", "location", "public_metrics", "verified"]
    tweet_fields = ["author_id", "created_at"]
    url = TWITTER_API_URL + "users{}={}&user.fields={}&expansions=pinned_tweet_id&tweet.fields={}" \
        .format(by, ",".join(list_to_use), ",".join(user_fields), ",".join(tweet_fields))
    return url


def filter_ugandan_users(users, save_all=False):
    """
    Filters the list of users to find only those whose location contains the word Uganda or Kampala
    :param users: list of users
    :param save_all: if True, don't filter
    :return: list of Ugandan users as described above
    """
    ugandan_users = []
    for user in users:
        if save_all or ("location" in user and ("Kampala" in user["location"] or "Uganda" in user["location"])):
            ugandan_users.append(user)
    return ugandan_users


def create_query(users=None, conversation_ids=None, entities=None, include_retweets=False):
    """
    Creates the query string depending on the parameters given.
    :param include_retweets: whether to include retweets
    :param users: list of users whose tweets to get.
    :param conversation_ids: the id of the conversation from which to get tweets
    :param entities: a list of entities (such as hashtags or annotations)
    :return: the query string
    """
    retweets = " -is:retweet" if not include_retweets else ""
    if users is not None:
        return " OR ".join("from:{}".format(user) for user in users) + retweets
    if conversation_ids is not None:
        return " OR ".join("conversation_id:{}".format(conv_id) for conv_id in conversation_ids) + retweets
    if entities is not None:
        return " OR ".join("entity:{}".format(entity) for entity in entities)
    return ""


def create_tweets_url(query, time_type="recent"):
    tweet_fields = "tweet.fields=id,author_id,conversation_id,text,created_at,in_reply_to_user_id,geo,public_metrics" \
                   ",source,referenced_tweets&expansions=author_id,in_reply_to_user_id,referenced_tweets.id" \
                   "&user.fields=name,username"
    url = TWITTER_API_URL + f"tweets/search/{time_type}?query={query}&{tweet_fields}"
    return url


def fetch_records(url, record_type="tweets", mode="collection"):
    bearer_token = auth("TWITTER_BEARER_TOKEN")
    headers = create_headers(bearer_token)

    records = []
    conversation_ids = set()
    user_ids = set()
    json_response, status_code = connect_to_endpoint(url, headers)
    records_retrieved = 0
    while True:
        if "data" in json_response:
            records.extend(json_response["data"])
        if len(records) >= 100:
            records_saved = 0
            records_retrieved += len(records)
            if record_type == "tweets":
                conversation_ids.update(extract_conversation_ids(records))
                user_ids.update(extract_author_ids(records))
                records_saved += save_records(records, record_type, mode)
            else:
                ugandans = filter_ugandan_users(records, mode != "collection")
                records_saved += save_records(ugandans, record_type, mode)
            records = []
            if records_saved == 0:
                break
        if "meta" in json_response and "next_token" in json_response["meta"]:
            new_url = url + "&next_token={}".format(json_response["meta"]["next_token"])
            json_response, status_code = connect_to_endpoint(new_url, headers)
        else:
            # TODO: Log number of requests made, and number of records collected (and type of record)
            break

    if len(records) > 0:
        records_retrieved += len(records)
        if record_type == "tweets":
            conversation_ids.update(extract_conversation_ids(records))
            user_ids.update(extract_author_ids(records))
            save_records(records, record_type, mode)
        else:
            ugandans = filter_ugandan_users(records, mode != "collection")
            save_records(ugandans, record_type, mode)

    print("Records collected: {}".format(records_retrieved))

    return records_retrieved, conversation_ids, user_ids, status_code


def extract_conversation_ids(tweets):
    tweets_with_replies = list(filter(lambda tweet: tweet["public_metrics"]["reply_count"] > 0, tweets))
    return set([tweet["conversation_id"] for tweet in tweets_with_replies])


def extract_author_ids(tweets):
    return set([tweet["author_id"] for tweet in tweets])


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        print("No more results: Status code: {} Response: {}".format(response.status_code, response.text))
        return {"data": [], "meta": {}}, response.status_code
    return response.json(), response.status_code
