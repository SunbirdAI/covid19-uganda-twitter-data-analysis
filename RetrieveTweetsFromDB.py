from pymongo import MongoClient
from collections import Counter
from prettytable import PrettyTable
import io
import json


def get_tweets_from_db(_collection):
    tweets_cursor = _collection.find()
    num_of_records = _collection.count_documents({})
    tweets_dict = dict()
    for tweet in tweets_cursor:
        tweets_dict[tweet['id_str']] = tweet

    num_of_unique_records = len(tweets_dict)
    print(num_of_records, num_of_unique_records)


# Find out how many twitter accounts are represented in this data.
def how_many_users(_collection):
    users_set = set()
    cursor = _collection.find()
    for tweet in cursor:
        users_set.add(tweet['user']['id_str'])

    print("Number of users: {}".format(len(users_set)))


def get_hashtags(_collection):
    cursor = _collection.find()
    return [hashtag['text'] for tweet in cursor for hashtag in tweet['entities']['hashtags']]


def get_tweet_text(tweet):
    if 'full_text' in tweet:
        return tweet['full_text']
    if 'text' in tweet:
        return tweet['text']
    return ""


def get_words(_collection):
    cursor = _collection.find()
    tweet_texts = []
    for tweet in cursor:
        if 'retweeted_status' in tweet:
            tweet_texts.append(get_tweet_text(tweet['retweeted_status']))
            # print("\n{}\n".format(get_tweet_text(tweet['retweeted_status'])))
        else:
            tweet_texts.append(get_tweet_text(tweet))
            text = get_tweet_text(tweet)
            # if "…" in text:
            #     print(tweet)
            # print("\n{}\n".format(get_tweet_text(tweet)))

    print(len(tweet_texts))

    return [word for text in tweet_texts for word in text.split()]


def get_usernames(_collection):
    cursor = _collection.find()
    tweet_usernames = []
    for tweet in cursor:
        tweet_usernames.append("{} {}".format(tweet["user"]["name"], tweet["user"]["screen_name"]))
    return tweet_usernames


def print_frequency_table(label, data):
    pt = PrettyTable(field_names=[label, 'Count'])
    c = Counter(data)
    [pt.add_row(kv) for kv in c.most_common()[:10]]
    pt.align[label], pt.align['Count'] = 'l', 'r'
    print(pt)


def write_to_json(_collection):
    cursor = _collection.find()
    with io.open('mask_tweets.json', 'w', encoding='utf-8') as f:
        tweets = []
        for tweet in cursor:
            tweet.pop('_id')
            tweets.append(tweet)
        f.write(json.dumps({"tweets": tweets}, ensure_ascii=False, indent=5))
        f.close()


if __name__ == '__main__':
    # Setup db
    client = MongoClient('localhost', 27017)
    db = client['twitter_db']
    collection = db.govt_mask_tweets

    # How many tweets are in the db and how many distinct users
    get_tweets_from_db(collection)
    how_many_users(collection)

    # Frequency analysis of words and hashtags
    print_frequency_table('Hashtag', get_hashtags(collection))
    print_frequency_table('Word', get_words(collection))
    print_frequency_table('Username', get_usernames(collection))

    # # Write to json file
    # write_to_json(db.mask_tweets)
