from __future__ import annotations

import time

from rot.core.logging import JsonlLogger
from rot.ingest.reddit_ingestor import RedditIngestor
from rot.trend.trend_store import TrendStore
from rot.trend.trend_engine import TrendEngine
from rot.extract.event_builder import EventBuilder
from rot.credibility.scorer import CredibilityScorer
from rot.reasoner.deepseek_client import DeepSeekReasoner
from rot.market.trade_builder import TradeBuilder
from rot.app.runner import PipelineRunner


def loop(interval_s: int = 20) -> None:
    logger = JsonlLogger(root="storage")

    ingestor = RedditIngestor(subreddits=["wallstreetbets", "stocks"], listing="hot", limit_per_sub=50)
    trend_engine = TrendEngine(store=TrendStore(), window_s=1800)
    event_builder = EventBuilder()
    cred = CredibilityScorer()
    reasoner = DeepSeekReasoner(api_key=None)
    trade_builder = TradeBuilder()

    runner = PipelineRunner(
        ingestor=ingestor,
        trend_engine=trend_engine,
        event_builder=event_builder,
        cred=cred,
        reasoner=reasoner,
        trade_builder=trade_builder,
        logger=logger,
    )

    while True:
        summary = runner.run_once()
        print(f"✅ {summary['run_id']} | snapshots={summary['snapshots']} candidates={summary['candidates']} ticker_candidates={summary['ticker_candidates']} events={summary['events']} ideas={summary['trade_ideas']} top_all={summary['top_signals']} top_ticker={summary['top_ticker_signals']}")
        print(
            f"✅ {summary['run_id']} | snapshots={summary['snapshots']} "
            f"candidates={summary['candidates']} ticker_candidates={summary['ticker_candidates']} "
            f"events={summary['events']} ideas={summary['trade_ideas']} "
            f"top_all={summary['top_signals']} top_ticker={summary['top_ticker_signals']}"
        )
        time.sleep(interval_s)


if __name__ == "__main__":
    loop()
