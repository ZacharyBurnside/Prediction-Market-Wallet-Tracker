"""
Myriad go-forward spider.
Fetches only events newer than the last stored tx_timestamp per market slug.
Schedule: every 15-30 minutes via PythonAnywhere tasks.
"""
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
        data  = resp.json()
        batch = data.get("data", [])
        last  = data.get("pagination", {}).get("last", 1)
        if not batch:
            break
        markets.extend(batch)
        if page >= last:
            break
        page += 1
        time.sleep(0.2)
    return markets

def get_last_timestamp(engine, slug):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MAX(tx_timestamp) FROM myriad_trades WHERE market_slug = :slug"),
            {"slug": slug}
        ).fetchone()
    return row[0] if row and row[0] else None

def fetch_new_events(slug, since_ts):
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

        stop = False
        for ev in events:
            ts = ev.get("timestamp")
            if since_ts and ts and ts <= since_ts:
                stop = True
                break
            rows.append({"ev": ev, "collected_at": collected_at})

        if stop or page >= last_page:
            break
        page += 1
        time.sleep(0.15)

    return rows, collected_at

def build_and_insert(engine, market, raw_rows):
    if not raw_rows:
        return 0
    outcomes   = market.get("outcomes", [])
    out0_title = outcomes[0]["title"] if len(outcomes) > 0 else None
    out0_price = outcomes[0]["price"] if len(outcomes) > 0 else None
    out1_title = outcomes[1]["title"] if len(outcomes) > 1 else None
    out1_price = outcomes[1]["price"] if len(outcomes) > 1 else None

    rows = []
    for item in raw_rows:
        ev  = item["ev"]
        ts  = ev.get("timestamp")
        rows.append({
            "collected_at":        item["collected_at"],
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
    started = datetime.utcnow()
    print(f"[{started}] Myriad spider starting")
    engine  = create_engine(DB_URL)
    markets = get_markets()
    print(f"  {len(markets)} markets found")

    total_inserted = 0
    for i, market in enumerate(markets):
        slug = market.get("slug")
        if not slug:
            continue
        since_ts = get_last_timestamp(engine, slug)
        raw_rows, collected_at = fetch_new_events(slug, since_ts)
        inserted = build_and_insert(engine, market, raw_rows)
        total_inserted += inserted
        if inserted:
            print(f"  [{i+1}] {slug}: +{inserted} new rows")

    elapsed = (datetime.utcnow() - started).total_seconds()
    print(f"[{datetime.utcnow()}] Done. {total_inserted} rows inserted in {elapsed:.1f}s")
    engine.dispose()

if __name__ == "__main__":
    run()
