from data_collection.twitterv2.db_utils import count_records, random_15_users
from data_collection.twitterv2.twitter_api import fetch_records, create_users_url, create_query, create_tweets_url
import time


def fetch_by_conversation_id(conv_ids, mode="collection"):
    query = create_query(conversation_ids=conv_ids)
    url = create_tweets_url(query)
    result = fetch_records(url, "tweets", mode=mode)
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
        print("Collecting tweets from random users....")
        user_list = random_15_users()
        response = fetch_by_random_users(user_list)
        while response[3] == 429 and response[0] == 0:
            # wait for 10 minutes and then make the request again. (This is because the rate limit will
            # have been reached)
            print("Rate limit exceeded, waiting for 10 minutes")
            time.sleep(600)
            response = fetch_by_random_users(user_list)

        conversation_ids = list(response[1])
        print("Number of Conversation ids retrieved: {}".format(len(conversation_ids)))

        # Fetch replies to the new conversation ids
        print("Collecting tweets/replies by the conversation ids...")
        author_ids = set()
        start_index = 0
        max_ids = 10
        while start_index < len(conversation_ids):
            response = fetch_by_conversation_id(
                conversation_ids[start_index:min(start_index + max_ids, len(conversation_ids))]
            )
            while response[3] == 429 and response[0] == 0:
                print("Rate limit exceeded, waiting for 10 minutes")
                time.sleep(600)
                response = fetch_by_conversation_id(
                    conversation_ids[start_index:min(start_index + max_ids, len(conversation_ids))]
                )
            start_index += max_ids
            author_ids.update(set(response[2]))

        # Save the new users
        if len(author_ids) > 0:
            print("Saving the new users...")
            author_ids = list(author_ids)
            start_index = 0
            max_users = 100
            while start_index < len(author_ids):
                response = save_new_users(
                    user_ids=author_ids[start_index:min(start_index + max_users, len(author_ids))]
                )
                while response[3] == 429 and response[0] == 0:
                    print("Rate limit exceeded, waiting for 10 minutes")
                    time.sleep(600)
                    response = save_new_users(
                        user_ids=author_ids[start_index:min(start_index + max_users, len(author_ids))]
                    )
                start_index += max_users
