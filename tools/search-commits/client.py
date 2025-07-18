import requests
import json
import time

from time import sleep
from pathlib import Path
from itertools import groupby
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor


def _q(keyword, **quals):
    # Build param q in query url
    quals = filter(lambda e: e[1] is not None, quals.items())
    quals = ' '.join(map(lambda e: f'{e[0]}:{e[1]}', quals))
    return f'{keyword} {quals}' if keyword else quals


def _url(base_url, **params):
    # Build complete query url
    params = {k: v for k, v in params.items() if v is not None}
    return f'{base_url}?{urlencode(params)}'


class Repo:
    def __init__(self, name, url):
        self.name = name
        self.url = url


class Commit:
    def __init__(self, repo, msg, url, files):
        self.repo = repo
        self.msg = msg
        self.url = url
        self.files = files


class Client:
    MAX_ITEM = 1000
    PER_PAGE = 100
    MAX_PAGE = 10

    SAFE = 1

    def __init__(self, *tokens):
        self._tokens = tokens
        self._headers = {'Authorization': f'token {tokens[0]}',
                         'Accept': 'application/vnd.github+json'}
        result = self._get_and_check('https://api.github.com/user')
        print(f'[LOGIN] {result["login"]}')

    def _check_limit(self, tag, demand):
        demand = demand + Client.SAFE
        # check current token
        result = self._get_and_check('https://api.github.com/rate_limit')
        limit = result['resources']
        remaining = limit[tag]['remaining']
        reset = limit[tag]['reset']
        if remaining >= demand:
            # use current token
            return
        # find another token with minimal sleep_time
        min_sleep_time = reset - time.time()
        for token in self._tokens:
            self._headers = {'Authorization': f'token {token}',
                             'Accept': 'application/vnd.github+json'}
            result = self._get_and_check('https://api.github.com/rate_limit')
            limit = result['resources']
            remaining = limit[tag]['remaining']
            reset = limit[tag]['reset']
            if remaining >= demand:
                # use this token
                result = self._get_and_check('https://api.github.com/user')
                print(f'[SWITCH] {result["login"]}')
                return
            sleep_time = reset - time.time()
            if sleep_time < min_sleep_time:
                min_sleep_time = sleep_time
                target_token = token
        # all tokens have to wait
        self._headers = {'Authorization': f'token {target_token}',
                         'Accept': 'application/vnd.github+json'}
        result = self._get_and_check('https://api.github.com/user')
        print(f'[SWITCH] {result["login"]}')
        if min_sleep_time > 0:
            # have to check because in rare case sleep_time could be less than 0
            print(f'[WAIT {int(sleep_time)} SECONDS]')
            sleep(sleep_time)

    def _timeout_retry(self, url, remain):
        if remain == 0:
            raise Exception(f'Time out: {url}')
        try:
            return requests.get(
                url, headers=self._headers, timeout=(5, 10))
        except requests.exceptions.Timeout:
            sleep(5)  # Wait in case of access limitations
            return self._timeout_retry(url, remain - 1)

    def _get_and_check(self, url):
        response = self._timeout_retry(url, 5)
        result = json.loads(response.text)
        if not response.ok:
            message = result['message']
            raise Exception(f'{message} ({response.url})')
        return result

    def search_repos(self, language=None, size=None, stars=None, sort=None, order=None, accept=None):
        repos = []
        for page in range(1, Client.MAX_PAGE + 1):
            url = _url('https://api.github.com/search/repositories',
                       q=_q('', size=size, language=language, stars=stars),
                       sort=sort, order=order, page=page, per_page=Client.PER_PAGE)
            self._check_limit('search', 1)
            result = self._get_and_check(url)
            items = result['items']
            if not items:
                break
            for item in items:
                repo = Repo(item['full_name'], item['html_url'])
                if not accept or accept(repo):
                    repos.append(repo)
            print(f'[REPO] [{len(repos)}] {url}')
        return repos

    def search_commits(self, keyword, repo, sort=None, order=None, accept_msg=None, accept_files=None):
        def build_commit(item):
            files_url = item['url']
            result = self._get_and_check(files_url)
            files = list(map(lambda file: file['filename'], result['files']))
            repo = item['repository']['full_name']
            msg = item['commit']['message']
            url = item['html_url']
            return Commit(repo, msg, url, files)

        commits = []
        for page in range(1, Client.MAX_PAGE + 1):
            url = _url('https://api.github.com/search/commits', q=_q(keyword, repo=repo),
                       sort=sort, order=order, page=page, per_page=Client.PER_PAGE)
            self._check_limit('search', 1)
            result = self._get_and_check(url)
            items = result['items']
            if not items:
                break
            accepted_items = []
            for item in items:
                msg = item['commit']['message']
                if not accept_msg or accept_msg(msg):
                    accepted_items.append(item)
            items = accepted_items
            if items:
                self._check_limit('core', len(items))
            num_accepted_commits = 0
            with ThreadPoolExecutor() as e:
                for commit in e.map(build_commit, items):
                    if not accept_files or accept_files(commit.files):
                        num_accepted_commits += 1
                        commits.append(commit)
            if num_accepted_commits:
                print(f'[COMMIT] [{repo}] [{len(commits)}] {url}')
        return commits

    def search_commits_2(self, keyword, repos, sort=None, order=None, accept_msg=None, accept_files=None):
        commits = []
        for repo in repos:
            commits.extend(self.search_commits(
                keyword, repo.name, sort=sort, order=order, accept_msg=accept_msg, accept_files=accept_files))
        return commits

    @staticmethod
    def dump_commits(commits, output):
        result = []
        commits = groupby(commits, lambda c: c.repo)
        for repo, commits_per_repo in commits:
            if not commits_per_repo:
                continue
            result.append({
                'repo': repo,
                'commits': list(map(lambda c: {
                    'msg': c.msg if len(c.msg) < 200 else f'{c.msg[:200]}...',
                    'url': c.url
                }, commits_per_repo))
            })
        text = json.dumps(result, indent=4)
        Path(output).write_text(text)
