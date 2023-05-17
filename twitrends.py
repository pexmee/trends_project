import logging
import os

import urllib3
from elasticsearch import logger as es_logger

from trend_modules.trend_bot import TrendBot


def twitter_trends():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.basicConfig(
        filename=os.path.join(
            os.path.dirname(__file__), "twitrends.log"
        ),
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    es_logger.setLevel(logging.INFO)

    logging.info("starting bot")
    with TrendBot() as bot:
        bot.listen()


if __name__ == "__main__":
    twitter_trends()
