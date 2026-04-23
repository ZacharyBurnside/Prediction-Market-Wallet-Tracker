import os
import requests
from flask import Flask, jsonify, request, send_file
from sqlalchemy import create_engine, text

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

engine = create_engine(DB_URL, pool_recycle=280)
app = Flask(__name__)

FRONTEND = '/home/zburnside/prediction_parket_tracker/whale_tracker/frontend/index.html'
HDRS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# ── Serve frontend ─────────────────────────────────────────────────────────────
@app.route('/')
def dashboard():
    return send_file(FRONTEND)

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

# ── Stats ──────────────────────────────────────────────────────────────────────
@app.route('/stats')
def stats():
    with engine.connect() as conn:
        poly_trades  = conn.execute(text("SELECT COUNT(*) FROM polymarket_trades")).scalar()
        lim_trades   = conn.execute(text("SELECT COUNT(*) FROM limitless_trades")).scalar()
        myr_trades   = conn.execute(text("SELECT COUNT(*) FROM myriad_trades")).scalar()
        poly_vol     = conn.execute(text("SELECT COALESCE(SUM(usd_cost), 0) FROM polymarket_trades")).scalar()
        poly_wallets = conn.execute(text("SELECT COUNT(DISTINCT proxyWallet) FROM polymarket_trades")).scalar()
        lim_wallets  = conn.execute(text("SELECT COUNT(DISTINCT profile_account) FROM limitless_trades")).scalar()
        myr_wallets  = conn.execute(text("SELECT COUNT(DISTINCT user_address) FROM myriad_trades")).scalar()
    return jsonify({
        "trades":  {"polymarket": poly_trades, "limitless": lim_trades, "myriad": myr_trades},
        "wallets": {"polymarket": poly_wallets, "limitless": lim_wallets, "myriad": myr_wallets},
        "polymarket_volume_usd": float(poly_vol)
    })

# ── Cross-platform wallets ─────────────────────────────────────────────────────
@app.route('/wallets/cross')
def cross_wallets():
    limit = request.args.get('limit', 100, type=int)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT wallet,
                   GROUP_CONCAT(DISTINCT platform ORDER BY platform) AS platforms,
                   COUNT(*)  AS trade_count,
                   SUM(vol)  AS total_vol
            FROM (
                SELECT LOWER(proxyWallet)     AS wallet, 'polymarket' AS platform, usd_cost                AS vol
                FROM polymarket_trades WHERE proxyWallet IS NOT NULL
                UNION ALL
                SELECT LOWER(profile_account) AS wallet, 'limitless'  AS platform, price * matched_size / 1e6 AS vol
                FROM limitless_trades WHERE profile_account IS NOT NULL
                UNION ALL
                SELECT LOWER(user_address)    AS wallet, 'myriad'     AS platform, value AS vol
                FROM myriad_trades WHERE user_address IS NOT NULL
            ) t
            GROUP BY wallet
            HAVING COUNT(DISTINCT platform) > 1
            ORDER BY total_vol DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return jsonify([
        {"wallet": r[0], "platforms": r[1].split(","), "trade_count": r[2], "total_vol": float(r[3] or 0)}
        for r in rows
    ])

# ── All wallets (filterable by platform, searchable) ──────────────────────────
@app.route('/wallets')
def wallets():
    limit    = request.args.get('limit',    100,  type=int)
    platform = request.args.get('platform', None)
    search   = request.args.get('search',   None)
    plat_clause   = "AND platform = :platform" if platform else ""
    search_clause = "AND wallet LIKE :search"   if search   else ""
    params = {"limit": limit}
    if platform: params["platform"] = platform
    if search:   params["search"]   = f"%{search.lower()}%"
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT wallet,
                   GROUP_CONCAT(DISTINCT platform ORDER BY platform) AS platforms,
                   COUNT(*)  AS trade_count,
                   SUM(vol)  AS total_vol
            FROM (
                SELECT LOWER(proxyWallet)     AS wallet, 'polymarket' AS platform, usd_cost                AS vol
                FROM polymarket_trades WHERE proxyWallet IS NOT NULL
                UNION ALL
                SELECT LOWER(profile_account) AS wallet, 'limitless'  AS platform, price * matched_size / 1e6 AS vol
                FROM limitless_trades WHERE profile_account IS NOT NULL
                UNION ALL
                SELECT LOWER(user_address)    AS wallet, 'myriad'     AS platform, value AS vol
                FROM myriad_trades WHERE user_address IS NOT NULL
            ) t
            WHERE 1=1 {plat_clause} {search_clause}
            GROUP BY wallet
            ORDER BY total_vol DESC
            LIMIT :limit
        """), params).fetchall()
    return jsonify([
        {"wallet": r[0], "platforms": r[1].split(","), "trade_count": r[2], "total_vol": float(r[3] or 0)}
        for r in rows
    ])

# ── Wallet profile (DB data + recent trades per platform) ─────────────────────
@app.route('/wallets/<address>')
def wallet_profile(address):
    addr = address.lower()
    with engine.connect() as conn:
        poly_agg = conn.execute(text("""
            SELECT COUNT(*), COALESCE(SUM(usd_cost), 0),
                   SUM(CASE WHEN side = 'BUY'  THEN 1 ELSE 0 END),
                   SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END)
            FROM polymarket_trades WHERE LOWER(proxyWallet) = :a
        """), {"a": addr}).fetchone()

        lim_agg = conn.execute(text("""
            SELECT COUNT(*), COALESCE(SUM(price * matched_size / 1e6), 0),
                   SUM(CASE WHEN side = 0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN side = 1 THEN 1 ELSE 0 END)
            FROM limitless_trades WHERE LOWER(profile_account) = :a
        """), {"a": addr}).fetchone()

        myr_agg = conn.execute(text("""
            SELECT COUNT(*), COALESCE(SUM(value), 0),
                   SUM(CASE WHEN action = 'buy'  THEN 1 ELSE 0 END),
                   SUM(CASE WHEN action = 'sell' THEN 1 ELSE 0 END)
            FROM myriad_trades WHERE LOWER(user_address) = :a
        """), {"a": addr}).fetchone()

        poly_recent = conn.execute(text("""
            SELECT title, side, usd_cost, slug, timestamp
            FROM polymarket_trades
            WHERE LOWER(proxyWallet) = :a
            ORDER BY timestamp DESC LIMIT 15
        """), {"a": addr}).fetchall()

        lim_recent = conn.execute(text("""
            SELECT market_slug, side_label, price, created_at
            FROM limitless_trades
            WHERE LOWER(profile_account) = :a
            ORDER BY created_at DESC LIMIT 15
        """), {"a": addr}).fetchall()

        myr_recent = conn.execute(text("""
            SELECT market_title, action, value, tx_datetime
            FROM myriad_trades
            WHERE LOWER(user_address) = :a
            ORDER BY tx_datetime DESC LIMIT 15
        """), {"a": addr}).fetchall()

    return jsonify({
        "address": address,
        "polymarket": {
            "trades": poly_agg[0], "volume": float(poly_agg[1]),
            "buys": int(poly_agg[2] or 0), "sells": int(poly_agg[3] or 0),
            "recent": [{"market": r[0], "side": r[1], "vol": float(r[2] or 0), "slug": r[3], "ts": r[4]} for r in poly_recent]
        },
        "limitless": {
            "trades": lim_agg[0], "volume": float(lim_agg[1]),
            "buys": int(lim_agg[2] or 0), "sells": int(lim_agg[3] or 0),
            "recent": [{"market": r[0], "side": r[1], "vol": float(r[2] or 0), "ts": str(r[3])} for r in lim_recent]
        },
        "myriad": {
            "trades": myr_agg[0], "volume": float(myr_agg[1]),
            "buys": int(myr_agg[2] or 0), "sells": int(myr_agg[3] or 0),
            "recent": [{"market": r[0], "side": r[1], "vol": float(r[2] or 0), "ts": str(r[3])} for r in myr_recent]
        }
    })

# ── All trades with pagination + filters ──────────────────────────────────────
@app.route('/trades')
def trades():
    limit    = request.args.get('limit',    100,  type=int)
    offset   = request.args.get('offset',   0,    type=int)
    platform = request.args.get('platform', None)
    side     = request.args.get('side',     None)
    min_vol  = request.args.get('min_vol',  0,    type=float)
    results  = []

    with engine.connect() as conn:
        if not platform or platform == 'polymarket':
            sc = f"AND side = '{side.upper()}'" if side else ""
            vc = f"AND usd_cost >= {min_vol}"   if min_vol > 0 else ""
            rows = conn.execute(text(f"""
                SELECT proxyWallet, side, usd_cost, title, slug, timestamp
                FROM polymarket_trades WHERE 1=1 {sc} {vc}
                ORDER BY timestamp DESC LIMIT :l OFFSET :o
            """), {"l": limit, "o": offset}).fetchall()
            results += [{"platform":"polymarket","wallet":r[0],"side":r[1],"vol":float(r[2] or 0),"market":r[3],"slug":r[4],"ts":r[5]} for r in rows]

        if not platform or platform == 'limitless':
            sc = f"AND side_label = '{side.lower()}'" if side else ""
            vc = f"AND price >= {min_vol}"             if min_vol > 0 else ""
            rows = conn.execute(text(f"""
                SELECT profile_account, side_label, price, market_slug, created_at
                FROM limitless_trades WHERE 1=1 {sc} {vc}
                ORDER BY created_at DESC LIMIT :l OFFSET :o
            """), {"l": limit, "o": offset}).fetchall()
            results += [{"platform":"limitless","wallet":r[0],"side":r[1],"vol":float(r[2] or 0),"market":r[3],"ts":str(r[4])} for r in rows]

        if not platform or platform == 'myriad':
            sc = f"AND action = '{side.lower()}'" if side else ""
            vc = f"AND value >= {min_vol}"        if min_vol > 0 else ""
            rows = conn.execute(text(f"""
                SELECT user_address, action, value, market_title, tx_datetime
                FROM myriad_trades WHERE 1=1 {sc} {vc}
                ORDER BY tx_datetime DESC LIMIT :l OFFSET :o
            """), {"l": limit, "o": offset}).fetchall()
            results += [{"platform":"myriad","wallet":r[0],"side":r[1],"vol":float(r[2] or 0),"market":r[3],"ts":str(r[4])} for r in rows]

    results.sort(key=lambda x: str(x.get("ts", "")), reverse=True)
    return jsonify({"data": results[:limit], "offset": offset, "limit": limit, "has_more": len(results) >= limit})

# ── External proxy: Polymarket user stats ─────────────────────────────────────
@app.route('/proxy/polymarket/stats/<address>')
def proxy_poly_stats(address):
    try:
        r = requests.get(
            f'https://data-api.polymarket.com/v1/user-stats?proxyAddress={address}',
            headers=HDRS, timeout=8
        )
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Polymarket open positions (all pages) ─────────────────────
@app.route('/proxy/polymarket/positions/<address>')
def proxy_poly_positions(address):
    try:
        all_positions = []
        offset = 0
        limit  = 500
        while True:
            r = requests.get(
                f'https://data-api.polymarket.com/positions'
                f'?user={address}&sortBy=CURRENT&sortDirection=DESC&sizeThreshold=.1&limit={limit}&offset={offset}',
                headers=HDRS, timeout=15
            )
            batch = r.json()
            if not batch or not isinstance(batch, list):
                break
            all_positions.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return jsonify(all_positions)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Polymarket closed positions (all pages) ───────────────────
@app.route('/proxy/polymarket/closed-positions/<address>')
def proxy_poly_closed_positions(address):
    try:
        all_positions = []
        offset = 0
        limit  = 500
        while True:
            r = requests.get(
                f'https://data-api.polymarket.com/closed-positions'
                f'?user={address}&sortBy=realizedpnl&sortDirection=DESC&limit={limit}&offset={offset}',
                headers=HDRS, timeout=15
            )
            batch = r.json()
            if not batch or not isinstance(batch, list):
                break
            all_positions.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return jsonify(all_positions)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Polymarket activity ───────────────────────────────────────
@app.route('/proxy/polymarket/activity/<address>')
def proxy_poly_activity(address):
    try:
        r = requests.get(
            f'https://data-api.polymarket.com/activity?user={address}&limit=30&offset=0',
            headers=HDRS, timeout=8
        )
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Limitless portfolio positions ─────────────────────────────
@app.route('/proxy/limitless/portfolio/<address>')
def proxy_lim_portfolio(address):
    try:
        r = requests.get(
            f'https://api.limitless.exchange/portfolio/{address}/positions',
            headers=HDRS, timeout=10
        )
        data = r.json()
        # Return only the clob list to match your pandas logic
        return jsonify(data.get('clob', []))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Myriad closed portfolio ───────────────────────────────────
@app.route('/proxy/myriad/portfolio/<address>')
def proxy_myr_portfolio(address):
    try:
        token_addr = (
            '0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d'
            ',0x55d398326f99059fF775485246999027B3197955'
        )
        r = requests.get(
            f'https://myriad.markets/portfolio'
            f'?address={address}&page=1&limit=30&networkId=56'
            f'&tokenAddress={token_addr}'
            f'&status=won,voided,lost,claimed,sold&sort=desc',
            headers=HDRS, timeout=10
        )
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── External proxy: Myriad user activity ─────────────────────────────────────
@app.route('/proxy/myriad/activity/<address>')
def proxy_myr_activity(address):
    try:
        r = requests.get(
            f'https://myriad.markets/userEvents'
            f'?address={address}&network_id=56&limit=30&page=1&only_relevant=true',
            headers=HDRS, timeout=8
        )
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)