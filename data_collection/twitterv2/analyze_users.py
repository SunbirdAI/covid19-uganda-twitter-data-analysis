from data_collection.twitterv2.db_utils import fetch_all_users
import io
import json


def find_users(number_followers=0):
    users = fetch_all_users()
    target_users = []
    # Fields: username, name, description, public_metrics{followers_count, following_count, tweet_count}, id, location,
    # created_at
    for user in users:
        if user['public_metrics']['followers_count'] >= number_followers:
            user.pop('_id')
            target_users.append(user)
    return target_users


def create_json_users(users):
    with io.open("10k_users.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps({"users": users}, ensure_ascii=False, indent=5))
        f.close()


if __name__ == '__main__':
    create_json_users(find_users(10000))
