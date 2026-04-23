import os
import re
import requests
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
SITEMAP_URL = "https://limitless.exchange/sitemap-markets.xml"
API_BASE    = "https://api.limitless.exchange/markets"
LIMIT       = 50

def get_slugs():
    r = requests.get(SITEMAP_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    links = re.findall(r'<loc>(.*?)</loc>', r.text)
    links = [l for l in links if re.search(r'\d', l)]
    return [l.rstrip('/').split('/')[-1] for l in links]

def fetch_all_events(slug):
    rows, page = [], 1
    while True:
        url = f"{API_BASE}/{slug}/events?page={page}&limit={LIMIT}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                break
            data = resp.json()
        except Exception as e:
            print(f"    error: {e}")
            break

        if isinstance(data, dict):
            total_pages = data.get("totalPages", 1)
            trades      = data.get("events", [])
        else:
            trades      = data
            total_pages = 1

        if not trades:
            break

        collected_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        for t in trades:
            profile = t.get("profile") or {}
            rows.append({
                "collected_at":    collected_at,
                "market_slug":     slug,
                "market_url":      f"https://limitless.exchange/markets/{slug}",
                "created_at":      t.get("createdAt", "")[:19].replace("T", " "),
                "tx_hash":         t.get("txHash"),
                "token_id":        t.get("tokenId"),
                "side":            t.get("side"),
                "side_label":      "buy" if t.get("side") == 0 else "sell",
                "price":           t.get("price"),
                "maker_amount":    t.get("makerAmount"),
                "taker_amount":    t.get("takerAmount"),
                "matched_size":    t.get("matchedSize"),
                "title":           t.get("title"),
                "profile_id":      profile.get("id"),
                "profile_account": profile.get("account"),
                "username":        profile.get("username"),
                "display_name":    profile.get("displayName"),
                "rank_name":       profile.get("rankName"),
            })

        print(f"    page {page}/{total_pages}: {len(trades)} events")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.2)

    return rows

def insert_rows(engine, rows):
    if not rows:
        return 0
    sql = text("""
        INSERT IGNORE INTO limitless_trades (
            collected_at, market_slug, market_url, created_at, tx_hash, token_id,
            side, side_label, price, maker_amount, taker_amount, matched_size,
            title, profile_id, profile_account, username, display_name, rank_name
        ) VALUES (
            :collected_at, :market_slug, :market_url, :created_at, :tx_hash, :token_id,
            :side, :side_label, :price, :maker_amount, :taker_amount, :matched_size,
            :title, :profile_id, :profile_account, :username, :display_name, :rank_name
        )
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return result.rowcount

def run():
    print(f"[{datetime.utcnow()}] Limitless historical ingest starting")
    engine = create_engine(DB_URL)
    slugs  = get_slugs()
    print(f"  {len(slugs)} market slugs found\n")

    total_inserted = 0
    for i, slug in enumerate(slugs):
        print(f"[{i+1}/{len(slugs)}] {slug}")
        rows     = fetch_all_events(slug)
        inserted = insert_rows(engine, rows)
        total_inserted += inserted
        print(f"  → {inserted} inserted ({len(rows)} fetched)")

    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM limitless_trades")).scalar()
    print(f"\nDone. {total_inserted} new rows inserted. DB total: {total}")
    engine.dispose()

if __name__ == "__main__":
    run()
