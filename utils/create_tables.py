import os
from sqlalchemy import create_engine, text

DB_URL = (
    f"mysql+pymysql://zburnside:{os.environ['MYSQL_PASSWORD']}"
    f"@zburnside.mysql.pythonanywhere-services.com/zburnside$polymarket"
)

engine = create_engine(DB_URL)

TABLES = {

"limitless_trades": """
CREATE TABLE IF NOT EXISTS limitless_trades (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    collected_at      DATETIME,
    market_slug       VARCHAR(255),
    market_url        VARCHAR(500),
    created_at        DATETIME,
    tx_hash           VARCHAR(100),
    token_id          VARCHAR(100),
    side              TINYINT COMMENT '0=buy 1=sell',
    side_label        VARCHAR(10),
    price             DECIMAL(10,6),
    maker_amount      BIGINT,
    taker_amount      BIGINT,
    matched_size      BIGINT,
    title             VARCHAR(500),
    profile_id        BIGINT,
    profile_account   VARCHAR(100)  COMMENT 'wallet address',
    username          VARCHAR(255),
    display_name      VARCHAR(255),
    rank_name         VARCHAR(50),
    UNIQUE KEY uq_tx_token (tx_hash, token_id),
    INDEX idx_wallet  (profile_account),
    INDEX idx_created (created_at),
    INDEX idx_slug    (market_slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"myriad_trades": """
CREATE TABLE IF NOT EXISTS myriad_trades (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    collected_at        DATETIME,
    market_slug         VARCHAR(255),
    market_title        VARCHAR(500),
    market_url          VARCHAR(500),
    market_state        VARCHAR(50),
    market_topics       VARCHAR(255),
    market_volume       DECIMAL(18,6),
    market_volume24h    DECIMAL(18,6),
    market_liquidity    DECIMAL(18,6),
    market_users        INT,
    market_published_at DATETIME,
    market_expires_at   DATETIME,
    market_token_symbol VARCHAR(20),
    outcome_0_title     VARCHAR(100),
    outcome_0_price     DECIMAL(10,6),
    outcome_1_title     VARCHAR(100),
    outcome_1_price     DECIMAL(10,6),
    tx_timestamp        BIGINT,
    tx_datetime         DATETIME,
    action              VARCHAR(10)  COMMENT 'buy or sell',
    outcome_title       VARCHAR(100),
    shares              DECIMAL(18,6),
    value               DECIMAL(18,6),
    user_address        VARCHAR(100) COMMENT 'wallet address',
    user_slug           VARCHAR(255),
    market_id           INT,
    UNIQUE KEY uq_tx (user_address, tx_timestamp, market_slug, outcome_title, shares),
    INDEX idx_wallet    (user_address),
    INDEX idx_datetime  (tx_datetime),
    INDEX idx_slug      (market_slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"polymarket_trades": """
CREATE TABLE IF NOT EXISTS polymarket_trades (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    transactionHash   VARCHAR(100) UNIQUE,
    proxyWallet       VARCHAR(100) COMMENT 'wallet address',
    side              VARCHAR(10),
    size              DECIMAL(18,6),
    price             DECIMAL(10,6),
    usd_cost          DECIMAL(18,6),
    conditionId       VARCHAR(100),
    title             VARCHAR(500),
    slug              VARCHAR(255),
    outcome           VARCHAR(100),
    outcomeIndex      INT,
    icon              VARCHAR(500),
    eventSlug         VARCHAR(255),
    name              VARCHAR(255),
    pseudonym         VARCHAR(255),
    bio               TEXT,
    profileImage      VARCHAR(500),
    is_sports         TINYINT DEFAULT 0,
    timestamp         BIGINT,
    INDEX idx_wallet  (proxyWallet),
    INDEX idx_time    (timestamp),
    INDEX idx_slug    (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

}

with engine.begin() as conn:
    for table_name, ddl in TABLES.items():
        conn.execute(text(ddl))
        print(f"  ✓ {table_name}")

print("\nAll 3 tables created successfully.")
engine.dispose()
