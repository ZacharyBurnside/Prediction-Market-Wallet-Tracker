"""
Polymarket go-forward spider.
Fetches only trades newer than the most recent transactionHash in the DB.
Schedule: every 10-15 minutes via PythonAnywhere tasks.
"""
import os
import re
import requests
import time
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'application/json',
    'Referer':    'https://polymarket.com/',
    'Origin':     'https://polymarket.com',
}

PAGE_SIZE = 500
MAX_PAGES = 20

SPORTS_ICON_PATTERN = re.compile(
    r'(?i)(nhl|nba|nfl|mlb|wnba|nascar|ncaa|ufc|mma|fifa|epl|mls|pga|atp|wta|'
    r'f1|formula.?1|rugby|cricket|tennis|golf|boxing|wrestling|'
    r'lakers|celtics|warriors|knicks|bulls|heat|nets|bucks|suns|'
    r'chiefs|patriots|cowboys|eagles|packers|49ers|ravens|bills|'
    r'yankees|dodgers|astros|braves|cubs|red.?sox|'
    r'sport|league|champion|trophy|cup|bowl|series|finals|playoffs?)'
)
SPORTS_SLUG_PATTERN = re.compile(
    r'(?i)(^nhl-|^nba-|^nfl-|^mlb-|^wnba-|^mls-|^ufc-|^ncaa-|'
    r'-vs-|-at-[a-z]{2,4}-\d{4}-|'
    r'-\d{4}-\d{2}-\d{2}$|'
    r'super.?bowl|stanley.?cup|world.?series|nba.?finals|'
    r'champion|playoff|bracket|draft)'
)
SPORTS_TITLE_PATTERN = re.compile(
    r'(?i)('
    r'\bnfl\b|\bnba\b|\bnhl\b|\bmlb\b|\bwnba\b|\bmls\b|\bufc\b|\bncaa\b|'
    r'\bufl\b|\bcfl\b|\bxfl\b|\bafl\b|\bnll\b|'
    r'premier league|champions league|europa league|la liga|serie a|bundesliga|ligue 1|'
    r'world cup|super bowl|stanley cup|world series|nba finals|'
    r'grand prix|formula 1|\bf1\b|indycar|nascar|'
    r'wimbledon|us open|french open|australian open|'
    r'\bplayoffs?\b|\bpostseason\b|\bbracket\b|\bdraft\b|\btrade deadline\b|'
    r'championship game|title game|bowl game|'
    r'mvp|cy young|heisman|norris trophy|hart trophy|vezina|'
    r'golden glove|silver slugger|rookie of the year|'
    r'\bvs\.?\s+[A-Z]|[A-Z][a-z]+\s+vs\.?\s+[A-Z]|'
    r'win the \d{4}|advance to|make the playoffs|clinch|'
    r'total (goals?|points?|runs?|touchdowns?|rebounds?|assists?)|'
    r'\bo\/u\b|\bover\/under\b|point spread|'
    r'passing yards|rushing yards|receiving yards|'
    r'home run|strikeout|rebound|assist|three.pointer'
    r')'
)

def is_sports(t):
    return bool(
        SPORTS_ICON_PATTERN.search(t.get('icon', '') or '') or
        SPORTS_SLUG_PATTERN.search(t.get('slug', '') or '') or
        SPORTS_SLUG_PATTERN.search(t.get('eventSlug', '') or '') or
        SPORTS_TITLE_PATTERN.search(t.get('title', '') or '')
    )

def get_newest_hash(engine):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT transactionHash FROM polymarket_trades ORDER BY timestamp DESC LIMIT 1")
        ).fetchone()
    return row[0] if row else None

def fetch_new_trades(stop_at_hash):
    all_trades, seen = [], set()
    for page in range(MAX_PAGES):
        params = {
            'takerOnly': 'true',
            'limit':     PAGE_SIZE,
            'offset':    page * PAGE_SIZE,
        }
        try:
            r = requests.get(
                'https://data-api.polymarket.com/trades',
                params=params, headers=HEADERS, timeout=15
            )
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            print(f"  page {page} error: {e}")
            break

        if not batch:
            break

        stop = False
        for t in batch:
            tx = t.get('transactionHash', '')
            if not tx or tx in seen:
                continue
            seen.add(tx)
            if stop_at_hash and tx == stop_at_hash:
                stop = True
                break
            all_trades.append(t)

        print(f"  page {page+1}: {len(all_trades)} new trades so far")

        if stop or len(batch) < PAGE_SIZE:
            break
        time.sleep(0.3)

    return all_trades

def insert_trades(engine, trades):
    if not trades:
        return 0
    rows = []
    for t in trades:
        size     = float(t.get('size',  0) or 0)
        price    = float(t.get('price', 0) or 0)
        usd_cost = size * price
        if usd_cost > 1_000_000:
            usd_cost /= 1_000_000
        rows.append({
            'transactionHash': t.get('transactionHash'),
            'proxyWallet':     t.get('proxyWallet'),
            'side':            t.get('side'),
            'size':            size,
            'price':           price,
            'usd_cost':        round(usd_cost, 6),
            'conditionId':     t.get('conditionId'),
            'title':           t.get('title'),
            'slug':            t.get('slug'),
            'outcome':         t.get('outcome'),
            'outcomeIndex':    t.get('outcomeIndex'),
            'icon':            t.get('icon'),
            'eventSlug':       t.get('eventSlug'),
            'name':            t.get('name'),
            'pseudonym':       t.get('pseudonym'),
            'bio':             t.get('bio'),
            'profileImage':    t.get('profileImage'),
            'is_sports':       1 if is_sports(t) else 0,
            'timestamp':       t.get('timestamp'),
        })
    sql = text("""
        INSERT IGNORE INTO polymarket_trades (
            transactionHash, proxyWallet, side, size, price, usd_cost,
            conditionId, title, slug, outcome, outcomeIndex,
            icon, eventSlug, name, pseudonym, bio, profileImage,
            is_sports, timestamp
        ) VALUES (
            :transactionHash, :proxyWallet, :side, :size, :price, :usd_cost,
            :conditionId, :title, :slug, :outcome, :outcomeIndex,
            :icon, :eventSlug, :name, :pseudonym, :bio, :profileImage,
            :is_sports, :timestamp
        )
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, rows)
    return result.rowcount

def run():
    started = datetime.utcnow()
    print(f"[{started}] Polymarket spider starting")
    engine      = create_engine(DB_URL)
    newest_hash = get_newest_hash(engine)
    print(f"  newest stored hash: {newest_hash or '(none)'}")
    trades   = fetch_new_trades(stop_at_hash=newest_hash)
    inserted = insert_trades(engine, trades)
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM polymarket_trades")).scalar()
    elapsed = (datetime.utcnow() - started).total_seconds()
    print(f"[{datetime.utcnow()}] Done. {inserted} new rows inserted. DB total: {total}. ({elapsed:.1f}s)")
    engine.dispose()

if __name__ == "__main__":
    run()
