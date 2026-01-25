import itertools
import random


_RANKS = "23456789TJQKA"
_SUITS = "SHDC"
_RANK_TO_VAL = {r: i + 2 for i, r in enumerate(_RANKS)}


def new_deck(seed: int | None = None) -> list[dict]:
    d = [{"r": r, "s": s} for s in _SUITS for r in _RANKS]
    rng = random.Random(seed)
    rng.shuffle(d)
    return d


def _is_straight(vals: list[int]) -> int | None:
    v = sorted(set(vals), reverse=True)
    if 14 in v:
        v.append(1)
    best = None
    run = 1
    for i in range(1, len(v)):
        if v[i - 1] - 1 == v[i]:
            run += 1
            if run >= 5:
                best = v[i - 4]
        else:
            run = 1
    return best


def _rank_5(cards: list[dict]) -> tuple:
    vals = sorted([_RANK_TO_VAL[c["r"]] for c in cards], reverse=True)
    suits = [c["s"] for c in cards]
    is_flush = len(set(suits)) == 1
    straight_high = _is_straight(vals)

    counts: dict[int, int] = {}
    for v in vals:
        counts[v] = counts.get(v, 0) + 1
    groups = sorted(((cnt, v) for v, cnt in counts.items()), reverse=True)
    cnts = sorted([c for c, _ in groups], reverse=True)

    if is_flush and straight_high is not None:
        return (8, straight_high)
    if cnts == [4, 1]:
        four = next(v for c, v in groups if c == 4)
        kicker = max(v for v in vals if v != four)
        return (7, four, kicker)
    if cnts == [3, 2]:
        three = next(v for c, v in groups if c == 3)
        pair = next(v for c, v in groups if c == 2)
        return (6, three, pair)
    if is_flush:
        return (5, *vals)
    if straight_high is not None:
        return (4, straight_high)
    if cnts == [3, 1, 1]:
        three = next(v for c, v in groups if c == 3)
        kickers = sorted([v for v in vals if v != three], reverse=True)
        return (3, three, *kickers)
    if cnts == [2, 2, 1]:
        pairs = sorted([v for c, v in groups if c == 2], reverse=True)
        kicker = max(v for v in vals if v not in pairs)
        return (2, pairs[0], pairs[1], kicker)
    if cnts == [2, 1, 1, 1]:
        pair = next(v for c, v in groups if c == 2)
        kickers = sorted([v for v in vals if v != pair], reverse=True)
        return (1, pair, *kickers)
    return (0, *vals)


def best_hand_rank(cards7: list[dict]) -> tuple:
    best = None
    for comb in itertools.combinations(cards7, 5):
        r = _rank_5(list(comb))
        if best is None or r > best:
            best = r
    return best or (0,)


def fmt_card(c: dict) -> str:
    return f"{c.get('r','?')}{c.get('s','?')}"

