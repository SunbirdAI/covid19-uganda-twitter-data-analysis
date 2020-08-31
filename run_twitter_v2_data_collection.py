from data_collection.twitterv2.db_utils import count_records, random_15_users
from data_collection.twitterv2.twitter_api import fetch_records, create_users_url, create_query, create_tweets_url
import time


def fetch_by_conversation_id(conversation_id):
    query = create_query(conversation_id=conversation_id)
    url = create_tweets_url(query)
    result = fetch_records(url, "tweets")
    return result


def fetch_by_random_users(users):
    query = create_query(users=users)
    url = create_tweets_url(query)
    result = fetch_records(url, "tweets")
    print("Collected {} tweets from random users".format(result[0]))
    return result


def save_new_users(usernames=None, user_ids=None):
    url = create_users_url(usernames=usernames, ids=user_ids)
    return fetch_records(url, "users")


def fetch_by_entity():
    pass


if __name__ == '__main__':
    current_iteration = 0
    default_user_list = ["MinOfHealthUG", "newvisionwire", "nbstv", "KagutaMuseveni", "ntvuganda", "observerug",
                         "MoICT_Ug", "bukeddetv", "DailyMonitor", "CanaryMugume", "fsnakazibwe", "KKariisa",
                         "nilepostnews", "AKasingye", "HEBobiwine", "kizzabesigye1", "cobbo3", "RedPepperUG",
                         "AmamaMbabazi", "UrbanTVUganda"]
    num_of_users_in_db = count_records("users")
    if num_of_users_in_db == 0:
        save_new_users(default_user_list)

    while True:
        # Fetch tweets by random users
        user_list = random_15_users()
        response = fetch_by_random_users(user_list)
        while response[0] == 0:
            # wait for 10 minutes and then make the request again. (This is because the rate limit will
            # have been reached)
            print("Rate limit exceeded, waiting for 10 minutes")
            time.sleep(600)
            response = fetch_by_random_users(user_list)

        conversation_ids = response[1]
        author_ids = response[2]

        # Save the new users
        response = save_new_users(user_ids=author_ids)
        while response[0] == 0:
            print("Rate limit exceeded, waiting for 10 minutes")
            time.sleep(600)
            response = save_new_users(user_ids=author_ids)

        # Fetch replies to the new conversation ids
        for conv_id in conversation_ids:
            response = fetch_by_conversation_id(conv_id)
            while response[0] == 0:
                print("Rate limit exceeded, waiting for 10 minutes")
                time.sleep(600)
                response = fetch_by_conversation_id(conv_id)

