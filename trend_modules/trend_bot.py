import datetime
import logging
import signal
import sys
import threading
from http.client import IncompleteRead
from threading import Thread
from time import sleep
from typing import Callable, Dict, Generator, List, Optional, Union
from dateutil.relativedelta import relativedelta
import os

import tweepy
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from trend_modules.settings import IndexDump, Settings

# Avoiding them coupling issues
TWEET_ID = "tweet_id"
LIKES = "likes"
RETWEET_COUNT = "retweet_count"
LIKE_COUNT = "like_count"
SOURCE = "_source"
HITS = "hits"
SUBSTRING = "substring"
TIMESTAMP = "timestamp"
DOC = "doc"

class Seppuku:
    """Makes threads commit suicide, gracefully"""

    def __init__(self):
        self.seppuku = False
        signal.signal(signal.SIGINT, self.instant_seppuku)
        signal.signal(signal.SIGTERM, self.instant_seppuku)

    def instant_seppuku(self, *args):
        self.seppuku = True


class TrendTweetListener(tweepy.StreamingClient):
    def __init__(
        self,
        bot,
        bearer_token,
        *,
        return_type=tweepy.Response,
        wait_on_rate_limit=False,
        **kwargs,
    ):
        super().__init__(
            bearer_token,
            return_type=return_type,
            wait_on_rate_limit=wait_on_rate_limit,
            **kwargs,
        )
        self.bot = bot

    def on_tweet(self, tweet):
        body: Dict[str, Union[str, int]] = {}

        if not tweet.text.startswith("RT @"):
            for rule in self.bot.settings.rules:
                match_ = rule.pattern.search(tweet.text)

                if match_:
                    logging.debug("adding tweet to index_dumps..")

                    body[TWEET_ID] = tweet.id
                    body["created_at"] = str(tweet.created_at)
                    body[TIMESTAMP] = datetime.datetime.now().isoformat()

                    body[LIKES] = tweet.public_metrics[LIKE_COUNT]
                    body[RETWEET_COUNT] = tweet.public_metrics[RETWEET_COUNT]
                    body[SUBSTRING] = match_.group(0).upper()

                    if rule.make_link:
                        body["google_link"] = (
                            "https://www.google.com/search?q=" + body[SUBSTRING]
                        )

                    self.bot.dump_findings(IndexDump(rule.index_name, body))

    def on_error(self, status):
        logging.error(f"error in tweet listener {status}")


class TrendThread(Thread):
    """
    Thread to be used by TrendBot.

    TrendThread will stop when sig_kill is set to False.
    """

    def __init__(self, name: str, target: Callable):
        Thread.__init__(self)
        self.target = target
        self.sig_kill = False
        self.name = name

    def run(self):
        logging.info(f"running thread {self.name}")

        while not self.sig_kill:
            self.target()

        logging.info(f"{self.name} stopped")


def threads_alive(threads: List[TrendThread]):
    """Returns whether all threads are alive."""
    return True in [thread.is_alive() for thread in threads]


class TrendBot:
    """
    Monitors Twitter activity matching a keyword and pattern.
    Sends matches to an Elasticsearch instance with the index set as 'tweet'.

    TrendBot defaults to not include tweet content nor username in dumps to Elasticsearch.
    """

    def __init__(self):
        self.settings = Settings()
        self.es = Elasticsearch(
            f"https://{self.settings.elastic_user}:{self.settings.elastic_pass}@{self.settings.elastic_ip}:{self.settings.elastic_port}",
            ca_certs=os.path.join(
            os.path.dirname(__file__), "../ca/http_ca.crt"
        ),
            verify_certs=True,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )
        self.timeblock = self._two_week_blocks()

        logging.info(f"elasticsearch alive: {self.es.ping()}")
        self.client = tweepy.Client(
            bearer_token=self.settings.twitter_bearer_token,
            consumer_key=self.settings.twitter_auth_key,
            consumer_secret=self.settings.twitter_auth_secret,
            access_token=self.settings.twitter_token_key,
            access_token_secret=self.settings.twitter_token_secret,
        )
        self.tweet_stream = TrendTweetListener(
            self,
            bearer_token=self.client.bearer_token,
            wait_on_rate_limit=True,
            chunk_size=512 * 4,
        )
        self.mutex = threading.Lock()
        self.index_dumps: List[
            IndexDump
        ] = []  # Used to hold index dumps if mutex is locked while updating

    def stream_twitter(
        self,
    ):
        """Streams twitter posts, filtering on provided keywords"""

        try:
            for rule in self.settings.rules:
                for keyword in rule.keyword:
                    self.tweet_stream.add_rules(tweepy.StreamRule(keyword))

            self.tweet_stream.filter(tweet_fields=self.settings.tweet_fields)

        except IncompleteRead as exc:
            logging.exception("IncompleteRead in stream_twitter:", exc_info=exc)

        except Exception as exc:
            logging.exception("unknown exception in stream_twitter:", exc_info=exc)

    def dump_findings(self, index_dump: Optional[IndexDump] = None):
        """
        Dumps all findings to Elasticsearch.
        If mutex is locked this only saves dump in memory for now.
        """
        if index_dump:
            self.index_dumps.append(index_dump)

        if self.mutex.acquire(False):
            try:
                logging.info("dumping findings")
            
                for index_dump in self.index_dumps:
                    if index_dump.body.get(TWEET_ID):
                        self.es.index(index=index_dump.name, body=index_dump.body)

                    self.index_dumps.clear()
            finally:
                self.mutex.release()

        else:
            logging.info("mutex was locked, passing")
            pass

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def update_tweets(self):
        """
        Updates all tweets saved in Elasticsearch if they need to be updated with new values.
        """

        def update_fields(
            obj: TrendBot, ids_names: Dict[int, str]
        ):
            try:
                tweet_info = obj.client.get_tweets(
                    ids=list(ids_names.keys()), tweet_fields=obj.settings.tweet_fields
                )
                for tweet in tweet_info.data:

                    for rule in self.settings.rules:
                        if rule.index_name != ids_names[tweet.id]:
                            continue

                        # Query Elasticsearch for the object with this tweet id
                        try:
                            res = obj.es.search(
                                index=rule.index_name,
                                body={"query": {"match": {TWEET_ID: tweet.id}}},
                            )
                        except NotFoundError as exc:
                            logging.debug(
                                "NotFoundError in update_fields, couldn't find index in database",
                                exc_info=exc,
                            )
                            continue

                        # Sanity check, we don't know if these fields are guaranteed to exist
                        try:
                            for hit in res[HITS][HITS]:
                                updated_fields: Dict[
                                    str, Dict[str, Union[int, str]]
                                ] = {DOC: {}}

                                if (
                                    hit[SOURCE][RETWEET_COUNT]
                                    < tweet.public_metrics[RETWEET_COUNT]
                                ):
                                    logging.info(
                                        f"updated {tweet.id} with retweets: {tweet.public_metrics[RETWEET_COUNT]}"
                                    )
                                    updated_fields[DOC][
                                        RETWEET_COUNT
                                    ] = tweet.public_metrics[RETWEET_COUNT]

                                if (
                                    hit[SOURCE][LIKES]
                                    < tweet.public_metrics[LIKE_COUNT]
                                ):
                                    logging.info(
                                        f"updated {tweet.id} with likes: {tweet.public_metrics[LIKE_COUNT]}"
                                    )
                                    updated_fields[DOC][
                                        LIKES
                                    ] = tweet.public_metrics[LIKE_COUNT]

                                if updated_fields:
                                    updated_fields[DOC][
                                        "updated_timestamp"
                                    ] = datetime.datetime.now().isoformat()

                                    obj.es.update(
                                        index=rule.index_name,
                                        id=hit["_id"],
                                        body=updated_fields,
                                    )

                        except KeyError as exc:
                            logging.exception(
                                "KeyError in update_fields - couldn't find expected result",
                                exc_info=exc,
                            )

            except Exception as exc:
                logging.exception("exception in update_fields", exc_info=exc)

        try:
            sleep(100)
            logging.info("updating tweets")
            ids_names: Dict[int, str] = {}

            # Better safe than sorry
            self.mutex.acquire()

            for id, index_name in self._tweet_cache():
                ids_names[int(id)] = index_name

                # Twitter only allows lookup of up to 100 entries at a time.
                if len(ids_names) == 100:
                    update_fields(self, ids_names)
                    ids_names.clear()

            if ids_names:
                update_fields(self, ids_names)

        except Exception as exc:
            logging.exception("exception in update_tweets:", exc_info=exc)

        finally:
            self.mutex.release()

            if self.index_dumps:
                self.dump_findings()

    @staticmethod
    def _run_parallel(*functions: Callable):
        """
        Starts all provided functions on separate TrendThreads.
        Terminates threads if SIGINT or SIGTERM is received.
        """

        logging.debug("starting threads")
        threads: List[TrendThread] = []
        name = 0
        killer = Seppuku()

        for func in functions:
            thread = TrendThread("thread#" + str(name), func)
            thread.daemon = True
            thread.start()
            threads.append(thread)
            name += 1

        while threads_alive(threads) and not killer.seppuku:
            try:
                for thread in threads:
                    if thread is not None and thread.is_alive():
                        thread.join(1)

            except KeyboardInterrupt as exc:
                logging.debug("received CTRL+C, killing threads..", exc_info=exc)
                for thread in threads:
                    thread.sig_kill = True

            else:
                if killer.seppuku:
                    logging.info("received SIGINT or SIGTERM")

                    for thread in threads:
                        thread.sig_kill = True
                    logging.info("death by seppuku")

    def listen(self):
        """listens to tweets with provided keyword"""
        try:
            self._run_parallel(self.stream_twitter, self.update_tweets)

        except Exception as exc:
            logging.exception("exception in listen:", exc_info=exc)
            sys.exit(1)

    def _tweet_cache(self) -> Union[Generator[str, None, None], List]:
        """Retrieves twitter IDs saved in elasticsearch"""
        # Will get 2 weeks at a time, 3 months back at maximum until current time/date.
        # This is to avoid getting results more than 10000 entries. Unlikely to happen though.
        gte, lte = next(self.timeblock) 

        for rule in self.settings.rules:
            resp = self.es.search(
                size=10000, # Max 10000 entries
                index=rule.index_name,
                query={
                    "range": {
                        TIMESTAMP: {
                            "gte": gte,
                            "lte": lte,
                        }
                    }
                },
            )
            for res in resp[HITS][HITS]:
                yield str(res[SOURCE][TWEET_ID]).strip(), rule.index_name
        
    @staticmethod
    def _two_week_blocks():
        # Since first block should be from 2 weeks ago to now, so we start
        # At 0
        n = 0

        while True:
            n = n % 14
            gte = ((datetime.datetime.now() - relativedelta(weeks=n)) - relativedelta(weeks=2)).isoformat()
            lte = (datetime.datetime.now() - relativedelta(weeks=n)).isoformat()
            n += 2
            yield gte,lte