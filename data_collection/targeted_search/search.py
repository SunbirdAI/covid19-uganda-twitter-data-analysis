from data_collection.twitterv2.twitter_api import create_query, create_tweets_url, fetch_records
from run_twitter_v2_data_collection import fetch_by_conversation_id
import time


def setup_query():
    usernames = ["MinOfHealthUG"]
    since_date = "2020-08-17"
    query = create_query(usernames)
    print("Collecting tweets")
    url = create_tweets_url(query) + "&since_id={}"
    response = fetch_records(url, mode="targeted")
    print("Status code: {}".format(response[3]))
    print("Fetched {} tweets".format(response[0]))
    print("Conversation ids: {}".format(len(response[1])))

    conversation_ids = list(response[1])
    print("Collecting tweets/replies by the conversation ids...")
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


if __name__ == '__main__':
    setup_query()
