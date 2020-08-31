from data_collection.db_utils import get_mongo_db_collection
from pymongo import ASCENDING, errors

DATABASE_NAME = "twitter_db"
TWEETS_COLLECTION_NAME = "tweets_v2_collection"
USERS_COLLECTION_NAME = "users_v2_collection"


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
    for rec in records:
        try:
            collection.insert_one(rec)
            successful_inserts += 1
        except errors.WriteError as e:
            print("Error inserting record: {}".format(e))
    return successful_inserts


def save_tweets(statuses):
    """
    Inserts the tweets into mongodb. Uses the insert_records function to ensure that there are no duplicates.
    :param statuses: tweets to insert
    """
    tweets_collection = setup_collection(DATABASE_NAME, TWEETS_COLLECTION_NAME)
    insert_records(tweets_collection, statuses)


def save_users(users):
    """
    Saves users into the db.
    :param users: the users to add
    """
    users_collection = setup_collection(DATABASE_NAME, USERS_COLLECTION_NAME)
    insert_records(users_collection, users)


def save_records(records, record_type):
    """
    Saves records to the db depending on the record type.
    :param records: the records/documents to save
    :param record_type: the type of record (either users or tweets)
    """
    if record_type == "tweets":
        save_tweets(records)
    else:
        save_users(records)


def count_records(record_type):
    collection = setup_collection(DATABASE_NAME,
                                  TWEETS_COLLECTION_NAME if record_type == "tweets" else USERS_COLLECTION_NAME)
    return collection.count_documents({})


def random_15_users():
    """
    Gets 15 random users from the database
    :return: a list of usernames
    """
    users_collection = setup_collection(DATABASE_NAME, USERS_COLLECTION_NAME)
    num_of_users = users_collection.count_documents({})
    rand_users = min(15, num_of_users)
    users_cursor = users_collection.aggregate([{"$sample": {"size": rand_users}}])
    users = []
    for user in users_cursor:
        users.append(user['username'])
    return users


def retrieve_tweets():
    tweets_cursor = get_mongo_db_collection(DATABASE_NAME, TWEETS_COLLECTION_NAME).find()
    tweets = []
    for tweet in tweets_cursor:
        tweets.append(tweet)
    return tweets
