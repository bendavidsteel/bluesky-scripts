import os

import atproto
import atproto.exceptions
import tqdm

def get_suggested_feeds(client, count):
    num_yielded = 0
    cursor = None
    while num_yielded < count:
        response = client.app.bsky.feed.get_suggested_feeds({"limit": 10, "cursor": cursor})
        for feed in response.feeds:
            yield feed
            num_yielded += 1
            if num_yielded >= count:
                break
        cursor = response.cursor

def get_feed(client, feed, count):
    num_yielded = 0
    cursor = None
    while num_yielded < count:
        response = client.app.bsky.feed.get_feed({"feed": feed.uri, "limit": 10, "cursor": cursor})
        for feed_post in response.feed:
            yield feed_post.post
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

    num_feeds = 100
    num_posts = 100

    feed_likes = {}
    
    feeds = get_suggested_feeds(client, num_feeds)
    for feed in tqdm.tqdm(feeds, total=num_feeds):
        success = False
        num_tries = 0
        while num_tries < max_tries:
            num_tries += 1
            try:
                num_likes = 0
                feed_posts = get_feed(client, feed, num_posts)
                for post in feed_posts:
                    num_likes += post.like_count
                success = True
                break
            except atproto.exceptions.NetworkError:
                continue
            except atproto.exceptions.RequestException:
                continue
            except atproto.exceptions.BadRequestError:
                continue
            except Exception:
                raise

        if success:
            feed_likes[feed.display_name] = num_likes

    print(feed_likes)

if __name__ == "__main__":
    main()
