"""Knowledge ranking utilities adapted for industrial monitoring cases."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

TOKEN_RE = re.compile(r"[a-z0-9_]+|[\u4e00-\u9fff]+", re.IGNORECASE)


def tokenize(text: str) -> List[str]:
    tokens: set[str] = set()
    for raw_token in TOKEN_RE.findall((text or "").lower()):
        token = raw_token.strip().lower()
        if len(token) <= 1:
            continue
        if re.fullmatch(r"[a-z0-9_]+", token):
            tokens.add(token)
            continue
        tokens.add(token)
        for size in (2, 3):
            if len(token) >= size:
                for idx in range(len(token) - size + 1):
                    tokens.add(token[idx : idx + size])
    return sorted(tokens)


def _score_case(case: Dict[str, Any], query: str, tokens: List[str]) -> float:
    title = str(case.get("title", "")).lower()
    summary = str(case.get("summary", "")).lower()
    content = str(case.get("content", "")).lower()
    root_cause = str(case.get("root_cause", "")).lower()
    tags = [str(item).lower() for item in case.get("tags", [])]
    scene_type = str(case.get("scene_type", "")).lower()
    source_type = str(case.get("source_type", "")).lower()

    score = 0.0
    if query and query in title:
        score += 20
    elif query and query in summary:
        score += 12

    for token in tokens:
        if token in title:
            score += 7
        elif token in root_cause:
            score += 6
        elif any(token in tag for tag in tags):
            score += 5
        elif token in summary or token in content:
            score += 3
        elif token in scene_type or token in source_type:
            score += 2

    usage_count = int(case.get("usage_count", 0) or 0)
    score += min(usage_count, 5)

    if source_type == "confirmed_label":
        score += 6

    return score


def rank_knowledge_cases(
    cases: Iterable[Dict[str, Any]],
    query: str,
    *,
    scene_type: str | None = None,
    top_k: int = 4,
) -> List[Dict[str, Any]]:
    normalized_query = (query or "").strip().lower()
    tokens = tokenize(normalized_query)
    ranked: List[Dict[str, Any]] = []
    for case in cases:
        if scene_type and str(case.get("scene_type", "")).lower() != scene_type.lower():
            continue
        score = _score_case(case, normalized_query, tokens)
        if score <= 0:
            continue
        enriched = dict(case)
        enriched["score"] = round(score, 2)
        ranked.append(enriched)

    ranked.sort(
        key=lambda item: (
            -float(item.get("score", 0)),
            -int(item.get("usage_count", 0) or 0),
            str(item.get("title", "")),
        )
    )
    return ranked[: max(top_k, 1)]
