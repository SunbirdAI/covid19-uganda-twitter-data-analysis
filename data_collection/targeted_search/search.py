from data_collection.twitterv2.twitter_api import create_query, create_tweets_url, fetch_records
from run_twitter_v2_data_collection import fetch_by_conversation_id, save_new_users
import time


MODE = "kcca"

USERNAMES_DICT = {
    "moh": ["MinOfHealthUG", "JaneRuth_Aceng", "WHOUganda"],
    "kcca": ["KCCASpox", "KCCAED", "KCCAUG"]
}


def perform_search():
    save_new_users(usernames=USERNAMES_DICT[MODE], mode=MODE)
    # since_date = "2020-08-17"
    query = create_query(USERNAMES_DICT[MODE], include_retweets=False)
    print("Collecting tweets")
    url = create_tweets_url(query)
    response = fetch_records(url, mode=MODE)
    print("Status code: {}".format(response[3]))
    print("Fetched {} tweets".format(response[0]))
    print("Conversation ids: {}".format(len(response[1])))

    conversation_ids = list(response[1])
    print("Collecting tweets/replies by the conversation ids...")
    start_index = 0
    author_ids = set()
    max_ids = 10
    while start_index < len(conversation_ids):
        response = fetch_by_conversation_id(
            conversation_ids[start_index:min(start_index + max_ids, len(conversation_ids))],
            mode=MODE
        )
        while response[3] == 429 and response[0] == 0:
            print("Rate limit exceeded, waiting for 10 minutes")
            time.sleep(600)
            response = fetch_by_conversation_id(
                conversation_ids[start_index:min(start_index + max_ids, len(conversation_ids))],
                mode=MODE
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
                    user_ids=author_ids[start_index:min(start_index + max_users, len(author_ids))],
                    mode=MODE
                )
                while response[3] == 429 and response[0] == 0:
                    print("Rate limit exceeded, waiting for 10 minutes")
                    time.sleep(600)
                    response = save_new_users(
                        user_ids=author_ids[start_index:min(start_index + max_users, len(author_ids))],
                        mode=MODE
                    )
                start_index += max_users


if __name__ == '__main__':
    perform_search()
