import os

import pandas as pd

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, 'data')
    post_path = os.path.join(data_dir_path, 'app.bsky.feed.post.parquet.gzip')
    post_df = pd.read_parquet(post_path)
    post_labels = post_df['labels'].dropna().values
    post_reply = post_df['reply'].dropna().values
    post_facets = post_df['facets'].dropna().values

    profile_path = os.path.join(data_dir_path, 'app.bsky.actor.profile.parquet.gzip')
    profile_df = pd.read_parquet(profile_path)
    profile_labels = profile_df['labels'].dropna().values

if __name__ == '__main__':
    main()