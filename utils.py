"""
Utilities file with common operations
"""
import twitter
import os
from dotenv import load_dotenv
from pymongo import MongoClient


def setup_twitter_api(retry=False):
    """Returns an instance of the Twitter object which can be used to make subsequent queries"""
    # Setup Twitter API
    load_dotenv()

    consumer_key = os.getenv("TWITTER_ACCESS_TOKEN")
    consumer_secret = os.getenv("TWITTER_TOKEN_SECRET")
    api_key = os.getenv("TWITTER_API_KEY")
    api_secret = os.getenv("TWITTER_API_SECRET")

    auth = twitter.oauth.OAuth(consumer_key, consumer_secret, api_key, api_secret)

    return twitter.Twitter(auth=auth, retry=False)


def get_mongo_db_collection(db_name, collection_name):

    client = MongoClient('localhost', 27017)
    db = client[db_name]

    return db[collection_name]
