CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id UUID NOT NULL UNIQUE,

    address TEXT NOT NULL UNIQUE,

    encrypted_private_key TEXT NOT NULL,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE token_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    wallet_address TEXT NOT NULL,
    mint_address TEXT NOT NULL,
    token_account_address TEXT UNIQUE NOT NULL,
    symbol TEXT,
    asset_type TEXT, -- spl | nft | native
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol TEXT UNIQUE NOT NULL,
    mint_address TEXT,
    decimals INT DEFAULT 9,
    asset_type TEXT, -- native | spl | nft
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id UUID,
    asset TEXT NOT NULL,

    debit NUMERIC(36,18) DEFAULT 0,
    credit NUMERIC(36,18) DEFAULT 0,

    reference TEXT, -- tx hash

    type TEXT, -- deposit | withdrawal | sweep | trade

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE hot_wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address TEXT UNIQUE NOT NULL,
    encrypted_private_key TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);