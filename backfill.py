from concurrent.futures import ThreadPoolExecutor
import os

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

def read_car_bytes(car_bytes, cid):
    data = atproto.CAR.from_bytes(car_bytes)
    return data

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
            # 'Authorization': f"Bearer {response.json()['accessJwt']}"
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

    # for repo in response.json()['repos']:
    #     yield repo
    # pds = "https://bsky.network"
    # client = atproto.Client(base_url=f'{pds}/xrpc')
    # repos = client.com.atproto.sync.list_repos().repos
    # for repo in repos:
    #     yield repo

def get_repo(base_url, did, head):
    # response = requests.request(
    #     "POST",
    #     "https://bsky.social/xrpc/com.atproto.server.createSession",
    #     headers={"Content-Type": "application/json"},
    #     json={"identifier": os.environ.get('BSKY_USERNAME'), "password": os.environ.get('BSKY_PASSWORD')}
    # )
    response = requests.request(
        "GET",
        f"{base_url}/com.atproto.sync.getRepo?did={did}",
        headers={"Content-Type": "application/json"},
        # json={"params": {"did": did}}
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch repo: {response.text}")
    
    data = read_car_bytes(response.content, head)
    
    return data
    # client = atproto.Client(base_url=base_url)
    # return client.com.atproto.sync.get_repo({"did": did})

def main():
    username = os.environ.get('BSKY_USERNAME')
    password = os.environ.get('BSKY_PASSWORD')

    # base_url = 'https://bsky.social/xrpc'
    # base_url = 'https://lionsmane.us-east.host.bsky.network/xrpc'
    base_url = 'https://bsky.network/xrpc'
    
    client = atproto.Client(base_url=base_url)
    # client.login(username, password)

    # base_url = 'https://public.api.bsky.app/xrpc'
    # client._base_url = base_url

    count = 10000
    repos = get_repos(base_url, count)

    repos = list(repos)[:count]

    data_dir_path = "./data"

    max_tries = 3
    dfs = {}
    for repo in tqdm.tqdm(repos):
        tries = 0
        while tries < max_tries:
            try:
                r = subprocess.run(["../../cookbook/go-repo-export/go-export-repo", "download-repo", repo['did']], check=True, capture_output=True, cwd=data_dir_path)
                path_to_car_file = f"{repo['did']}.car"
                r = subprocess.run(["../../cookbook/go-repo-export/go-export-repo", "unpack-records", path_to_car_file], check=True, capture_output=True, cwd=data_dir_path)
            except subprocess.CalledProcessError as e:
                tries += 1
                continue
            else:
                break

if __name__ == '__main__':
    main()
