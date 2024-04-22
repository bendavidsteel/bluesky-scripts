import os

import atproto
import atproto.exceptions
import tqdm

def get_moderation_events(client: atproto.Client, count):
    num_yielded = 0
    cursor = None
    while num_yielded < count:
        response = client.tools.ozone.moderation.query_events({"limit": 10, "cursor": cursor})
        for feed in response.feeds:
            yield feed
            num_yielded += 1
            if num_yielded >= count:
                break
        cursor = response.cursor


def main():
    username = os.environ.get('BSKY_USERNAME')
    password = os.environ.get('BSKY_PASSWORD')

    client = atproto.Client(base_url='https://bsky.social')
    client.login(username, password)

    max_tries = 3

    num_events = 100

    feed_likes = {}
    
    moderation_events = get_moderation_events(client, num_feeds)
    for event in moderation_events:
        print(f"Event: {event}")

if __name__ == "__main__":
    main()
