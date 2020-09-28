from data_collection.db_utils import get_mongo_db_collection
from pymongo import ASCENDING, errors

DATABASE_NAME = "twitter_db"


def get_collection_name(mode):
    """
    Returns the mongo-db collections to use based on the mode
    :param mode: The data collection mode e.g moh, kcca
    :return: the name of the collections to use
    """
    if mode == "collection":
        return {"tweets": "tweets_v2_collection", "users": "users_v2_collection"}
    return {"tweets": "tweets_{}".format(mode), "users": "users_{}".format(mode)}


def setup_collection(db_name, collection_name):
    """
    Sets up the collection by creating an index for it if it doesn't exist
    :param db_name: name of the database
    :param collection_name: name of the collection
    :return: the collection
    """
    collection = get_mongo_db_collection(db_name, collection_name)
    if len(collection.index_information()) < 2:
        collection.create_index([('id', ASCENDING)], unique=True)
    return collection


def insert_records(collection, records):
    """
    Inserts records into the collection. (Records can be users or tweets for this projects use-case)
    :param collection: the collection into which to insert
    :param records: the records to insert (a list of users or tweets)
    :return: number of successful inserts
    """
    successful_inserts = 0
    duplicates = 0
    for rec in records:
        try:
            collection.insert_one(rec)
            successful_inserts += 1
        except errors.WriteError as _:
            duplicates += 1
    print("Successful inserts: {} Duplicates: {}".format(successful_inserts, duplicates))
    return successful_inserts


def save_tweets(statuses, collection_name):
    """
    Inserts the tweets into mongodb. Uses the insert_records function to ensure that there are no duplicates.
    :param statuses: tweets to insert
    :param collection_name: the name of the collection into which to insert
    """
    tweets_collection = setup_collection(DATABASE_NAME, collection_name)
    return insert_records(tweets_collection, statuses)


def save_users(users, collection_name):
    """
    Saves users into the db.
    :param users: the users to add
    :param collection_name: the name of the mongo-db collection into which to add
    """
    users_collection = setup_collection(DATABASE_NAME, collection_name)
    return insert_records(users_collection, users)


def save_records(records, record_type, mode="collection"):
    """
    Saves records to the db depending on the record type.
    :param records: the records/documents to save
    :param record_type: the type of record (either users or tweets)
    :param mode: the mode in which this function was called. Used to determine which collection
    """
    collection_name = get_collection_name(mode)
    if record_type == "tweets":
        return save_tweets(records, collection_name[record_type])
    else:
        return save_users(records, collection_name[record_type])


def count_records(record_type, mode="collection"):
    collections = get_collection_name(mode)
    collection_name = collections["tweets"] if record_type == "tweets" else collections["users"]
    collection = setup_collection(DATABASE_NAME, collection_name)
    return collection.count_documents({})


def random_15_users(mode="collection"):
    """
    Gets 15 random users from the database
    :return: a list of usernames
    """
    collection_name = get_collection_name(mode)
    users_collection = setup_collection(DATABASE_NAME, collection_name["users"])
    num_of_users = users_collection.count_documents({})
    rand_users = min(15, num_of_users)
    users_cursor = users_collection.aggregate([{"$sample": {"size": rand_users}}])
    users = []
    for user in users_cursor:
        users.append(user['username'])
    return users


def retrieve_tweets(mode="collection"):
    collection_name = get_collection_name(mode)
    tweets_cursor = get_mongo_db_collection(
        DATABASE_NAME,
        collection_name["tweets"]
    ).find()
    tweets = []
    for tweet in tweets_cursor:
        tweets.append(tweet)
    return tweets


def fetch_all_users(mode="collection"):
    collection_name = get_collection_name(mode)
    user_cursor = get_mongo_db_collection(
        DATABASE_NAME,
        collection_name["users"]
    ).find({})
    users = [user for user in user_cursor]
    return users


def fetch_user_by_id(user_id, mode="collection"):
    collection_name = get_collection_name(mode)
    user_cursor = get_mongo_db_collection(
        DATABASE_NAME,
        collection_name["users"]
    ).find({'id': user_id})
    users = [user for user in user_cursor]
    return users


def fetch_tweet_by_id(tweet_id, mode="collection"):
    collection_name = get_collection_name(mode)
    tweet_cursor = get_mongo_db_collection(
        DATABASE_NAME,
        collection_name["tweets"]
    ).find({'id': tweet_id})
    tweets = [tweet for tweet in tweet_cursor]
    return tweets
