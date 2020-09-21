from urllib.parse import unquote
from utils import setup_twitter_api, get_mongo_db_collection


def create_query_string(lists, date_range):
    lists_or = [" OR ".join(lst) for lst in lists]
    q_string = "(" + ") (".join(lists_or[:2]) + ") OR (" + ") (".join(lists_or[2:])
    return "{} since:{} until:{}".format(q_string, date_range["since"], date_range["until"])


def get_more_results(tweets):
    num_of_requests_made = 0
    statuses = tweets['statuses']
    search_results = tweets
    tweets_retrieved = 0
    while True:
        print("Number of tweets so far: ", len(statuses) + tweets_retrieved)
        if len(statuses) >= 100:
            save_tweets_to_mongodb(statuses)
            tweets_retrieved += len(statuses)
            statuses = []
        try:
            next_results = search_results['search_metadata']['next_results']
        except KeyError as e:
            break
        kwargs = dict([kv.split("=") for kv in unquote(next_results[1:]).split("&")])
        try:
            search_results = twitter_api.search.tweets(**kwargs)
            if len(search_results['statuses']) == 0:
                break
            statuses += search_results['statuses']
            num_of_requests_made += 1
        except KeyError as e:
            print("{}\nRetry: {}".format(e, twitter_api.retry))
            break
        except Exception as e:
            print("{}".format(e))
            break

    if len(statuses) > 0:
        save_tweets_to_mongodb(statuses)
    print("Number of requests before no results: ", num_of_requests_made + 1)


def get_tweets(query_string):
    tweets = twitter_api.search.tweets(q=query_string, tweet_mode='extended')

    # Get more results
    get_more_results(tweets)

    print(tweets)

    print("Finished retrieving '{}' tweets".format(query_string))


def save_tweets_to_mongodb(statuses):
    result = _mask_tweets.insert_many(statuses)
    print("Result of insert: {0}".format(result.inserted_ids))


if __name__ == '__main__':
    # Setup Twitter API
    twitter_api = setup_twitter_api(retry=True)

    # Setup MongoDB
    _mask_tweets = get_mongo_db_collection('twitter_db', 'mask_tweets')

    # Run query
    words_list = ["mask", "masks"]
    hashtags_list = ["#m7address", "#covid19ug", "#covid19UG", "#COVID19UG", "#StaySafeUG"]
    accounts_list = ["from:MinOfHealthUG", "from:newvisionwire", "from:nbstv", "from:KagutaMuseveni"]
    # locations_list = ["place:Uganda", "bio_location:Kampala"]

    _date_range = {"since": "2020-09-9", "until": "2020-09-11"}

    _query_string = create_query_string([words_list, hashtags_list, accounts_list], _date_range)
    print("Query: ", _query_string)
    get_tweets(_query_string)
