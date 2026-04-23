import os
import requests
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

MARKETS_BASE   = "https://myriad.markets/v2/markets"
EVENTS_BASE    = "https://myriad.markets/marketEvents"
EVENTS_LIMIT   = 50
MARKETS_PARAMS = {
    "network_id":    56,
    "limit":         50,
    "sortBy":        "featured_at",
    "order":         "desc",
    "state":         "open",
    "token_address": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d,0x55d398326f99059fF775485246999027B3197955",
    "min_duration":  3601,
}

def get_markets():
    markets, page = [], 1
    while True:
        MARKETS_PARAMS["page"] = page
        resp = requests.get(MARKETS_BASE, params=MARKETS_PARAMS, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data     = resp.json()
        batch    = data.get("data", [])
        last     = data.get("pagination", {}).get("last", 1)
        if not batch:
            break
        markets.extend(batch)
        print(f"  markets page {page}/{last}: {len(batch)}")
        if page >= last:
            break
        page += 1
        time.sleep(0.2)
    return markets

def fetch_events(slug):
    rows, page = [], 1
    collected_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    while True:
        params = {"marketId": slug, "limit": EVENTS_LIMIT, "page": page, "only_relevant": "true"}
        try:
            resp = requests.get(EVENTS_BASE, params=params, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                break
            data = resp.json()
        except Exception as e:
            print(f"    error: {e}")
            break

        events    = data.get("data", [])
        last_page = data.get("pagination", {}).get("last", 1)
        if not events:
            break
        rows.extend(events)
        print(f"    page {page}/{last_page}: {len(events)} events")
        if page >= last_page:
            break
        page += 1
        time.sleep(0.15)
    return rows, collected_at

def build_rows(market, events, collected_at):
    outcomes   = market.get("outcomes", [])
    out0_title = outcomes[0]["title"] if len(outcomes) > 0 else None
    out0_price = outcomes[0]["price"] if len(outcomes) > 0 else None
    out1_title = outcomes[1]["title"] if len(outcomes) > 1 else None
    out1_price = outcomes[1]["price"] if len(outcomes) > 1 else None

    rows = []
    for ev in events:
        ts  = ev.get("timestamp")
        rows.append({
            "collected_at":        collected_at,
            "market_slug":         market.get("slug"),
            "market_title":        market.get("title"),
            "market_url":          f"https://myriad.markets/markets/{market.get('slug')}",
            "market_state":        market.get("state"),
            "market_topics":       ", ".join(market.get("topics", [])),
            "market_volume":       market.get("volume"),
            "market_volume24h":    market.get("volume24h"),
            "market_liquidity":    market.get("liquidity"),
            "market_users":        market.get("users"),
            "market_published_at": (market.get("publishedAt") or "")[:19].replace("T", " ") or None,
            "market_expires_at":   (market.get("expiresAt") or "")[:19].replace("T", " ") or None,
            "market_token_symbol": market.get("token", {}).get("symbol"),
            "outcome_0_title":     out0_title,
            "outcome_0_price":     out0_price,
            "outcome_1_title":     out1_title,
            "outcome_1_price":     out1_price,
            "tx_timestamp":        ts,
            "tx_datetime":         datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None,
            "action":              ev.get("action"),
            "outcome_title":       ev.get("outcomeTitle"),
            "shares":              ev.get("shares"),
            "value":               ev.get("value"),
            "user_address":        ev.get("userAddress"),
            "user_slug":           ev.get("userSlug"),
            "market_id":           ev.get("marketId"),
        })
    return rows

def insert_rows(engine, rows):
    if not rows:
        return 0
    sql = text("""
        INSERT IGNORE INTO myriad_trades (
            collected_at, market_slug, market_title, market_url, market_state,
            market_topics, market_volume, market_volume24h, market_liquidity,
            market_users, market_published_at, market_expires_at, market_token_symbol,
            outcome_0_title, outcome_0_price, outcome_1_title, outcome_1_price,
            tx_timestamp, tx_datetime, action, outcome_title,
            shares, value, user_address, user_slug, market_id
        ) VALUES (
            :collected_at, :market_slug, :market_title, :market_url, :market_state,
            :market_topics, :market_volume, :market_volume24h, :market_liquidity,
            :market_users, :market_published_at, :market_expires_at, :market_token_symbol,
            :outcome_0_title, :outcome_0_price, :outcome_1_title, :outcome_1_price,
            :tx_timestamp, :tx_datetime, :action, :outcome_title,
            :shares, :value, :user_address, :user_slug, :market_id
        )
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return result.rowcount

def run():
    print(f"[{datetime.utcnow()}] Myriad historical ingest starting")
    engine  = create_engine(DB_URL)
    markets = get_markets()
    print(f"\n  {len(markets)} markets found\n")

    total_inserted = 0
    for i, market in enumerate(markets):
        slug = market.get("slug")
        if not slug:
            continue
        print(f"[{i+1}/{len(markets)}] {slug}")
        events, collected_at = fetch_events(slug)
        rows     = build_rows(market, events, collected_at)
        inserted = insert_rows(engine, rows)
        total_inserted += inserted
        print(f"  → {inserted} inserted ({len(rows)} fetched)")

    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM myriad_trades")).scalar()
    print(f"\nDone. {total_inserted} new rows inserted. DB total: {total}")
    engine.dispose()

if __name__ == "__main__":
    run()
