from __future__ import annotations

import re
from typing import List

from rot.core.types import Evidence, Event, TrendCandidate
from rot.market.enricher import NON_EQUITY_TOKENS, ALIAS_MAP

# Matches $TSLA or TSLA
_TICKER_RE = re.compile(r"(?:\$([A-Z]{1,5})\b|\b([A-Z]{1,5})\b)")


class EventBuilder:
    def extract_entities(self, title: str, body: str) -> List[str]:
        text = f"{title}\n{body}"
        matches = _TICKER_RE.findall(text)

        # Prefer explicit $TICKER mentions
        dollar = [a for (a, b) in matches if a]
        bare = [b for (a, b) in matches if b]
        raw = dollar if dollar else bare

        out: List[str] = []
        for s in raw:
            s = ALIAS_MAP.get(s.upper(), s.upper())

            if s in NON_EQUITY_TOKENS:
                continue
            if len(s) == 1:
                continue

            out.append(s)

        return sorted(set(out))[:5]

    def from_candidate(self, c: TrendCandidate) -> List[Event]:
        post = c.snapshot.post
        tickers = self.extract_entities(post.title, post.selftext)

        ev = Event(
            event_type="other",
            entities=tickers,
            stance="unknown",
            time_horizon="unknown",
            evidence=[
                Evidence(
                    post_id=post.id,
                    permalink=post.permalink,
                    subreddit=post.subreddit,
                    excerpt=post.title[:200],
                )
            ],
            confidence=0.3,
            meta={
                "trend_score": c.trend_score,
                "features": c.features,
            },
        )

        return [ev]
