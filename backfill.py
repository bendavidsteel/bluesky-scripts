from concurrent.futures import ThreadPoolExecutor
import collections
import datetime
import json
import os
import shutil

import atproto
import atproto.exceptions
import pandas as pd
import requests
import subprocess
import tqdm

def thread_map(*args, function=None, num_workers=10):
    assert function is not None, "function must be provided"
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        return list(executor.map(function, *args))

def get_repos(base_url, count):
    try:
        client = atproto.Client(base_url=base_url)
        num_yielded = 0
        cursor = None
        while num_yielded < count:
            response = client.com.atproto.sync.list_repos({"limit": 100, "cursor": cursor})
            for repo in response.repos:
                yield repo
                num_yielded += 1
                if num_yielded >= count:
                    break
            cursor = response.cursor
    except atproto.exceptions.ModelError:

        url = f"{base_url}/com.atproto.sync.listRepos"

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }

        amount_yielded = 0
        cursor = None
        while amount_yielded < count:
            response = requests.request(
                "GET", 
                url, 
                headers=headers, 
                json={"limit": 100, "cursor": cursor}
            )

            if response.status_code != 200:
                raise Exception(f"Failed to fetch repos: {response.text}")
            
            ret = response.json()
            cursor = ret['cursor']
            amount_yielded += len(ret['repos'])

            for repo in ret['repos']:
                yield repo


def main():
    base_url = 'https://bsky.network/xrpc'

    count = 100000
    repos = get_repos(base_url, count)

    repos = list(repos)[:count]

    data_dir_path = "./data"

    data_types = ["app.bsky.actor.profile", "app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.graph.follow"]

    max_tries = 3
    current_batch = 0
    batch_size = 1000
    dfs = collections.defaultdict(list)
    for repo in tqdm.tqdm(repos):
        # TODO make this multi-threadedd
        current_batch += 1
        tries = 0
        while tries < max_tries:
            try:
                r = subprocess.run(["../../cookbook/go-repo-export/go-export-repo", "download-repo", repo['did']], check=True, capture_output=True, cwd=data_dir_path)
                path_to_car_file = f"{repo['did']}.car"
                r = subprocess.run(["../../cookbook/go-repo-export/go-export-repo", "unpack-records", path_to_car_file], check=True, capture_output=True, cwd=data_dir_path)
                os.remove(os.path.join(data_dir_path, path_to_car_file))
                for data_type in data_types:
                    if not os.path.exists(os.path.join(data_dir_path, repo['did'], data_type)):
                        continue
                    for filename in os.listdir(os.path.join(data_dir_path, repo['did'], data_type)):
                        with open(os.path.join(data_dir_path, repo['did'], data_type, filename)) as f:
                            data = json.load(f)
                            dfs[data_type].append(data)
                with open(os.path.join(data_dir_path, repo['did'], "_commit.json")) as f:
                    commit = json.load(f)
                    dfs["commit"].append(commit)
                shutil.rmtree(os.path.join(data_dir_path, repo['did']))
            except subprocess.CalledProcessError as e:
                tries += 1
                continue
            else:
                break

        if current_batch % batch_size == 0:
            for data_type, data in dfs.items():
                df = pd.DataFrame(data)
                df.to_parquet(os.path.join(data_dir_path, f"{data_type}_{datetime.datetime.now().isoformat()}.parquet.gzip"), compression="gzip")
            dfs = collections.defaultdict(list)

if __name__ == '__main__':
    main()
