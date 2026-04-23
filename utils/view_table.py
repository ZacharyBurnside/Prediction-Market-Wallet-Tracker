import os
import pandas as pd
from sqlalchemy import create_engine

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

engine = create_engine(DB_URL)

# ── Config ─────────────────────────────────────────────────────────────────────
TABLE  = "polymarket_trades"   # change to: limitless_trades / myriad_trades
LIMIT  = 100                   # number of rows to preview
# ──────────────────────────────────────────────────────────────────────────────

df = pd.read_sql(f"SELECT * FROM {TABLE} ORDER BY timestamp DESC LIMIT {LIMIT}", engine)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 40)
pd.set_option("display.width", 200)

print(f"\nTable: {TABLE}")
print(f"Rows returned: {len(df)}")
print(f"Columns: {list(df.columns)}\n")
print(df.to_string(index=False))

engine.dispose()
