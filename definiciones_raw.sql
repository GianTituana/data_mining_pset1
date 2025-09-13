CREATE SCHEMA raw;

CREATE TABLE raw.qb_customer (
                    id VARCHAR(50) PRIMARY KEY,
                    payload JSONB,
                    ingested_at_utc TIMESTAMPTZ,
                    extract_window_start_utc TIMESTAMPTZ,
                    extract_window_end_utc TIMESTAMPTZ,
                    page_number INTEGER,
                    page_size INTEGER,
                    request_payload JSONB
                );
CREATE TABLE raw.qb_invoices (
                    id VARCHAR(50) PRIMARY KEY,
                    payload JSONB,
                    ingested_at_utc TIMESTAMPTZ,
                    extract_window_start_utc TIMESTAMPTZ,
                    extract_window_end_utc TIMESTAMPTZ,
                    page_number INTEGER,
                    page_size INTEGER,
                    request_payload JSONB
                );

CREATE TABLE raw.qb_item (
                    id VARCHAR(50) PRIMARY KEY,
                    payload JSONB,
                    ingested_at_utc TIMESTAMPTZ,
                    extract_window_start_utc TIMESTAMPTZ,
                    extract_window_end_utc TIMESTAMPTZ,
                    page_number INTEGER,
                    page_size INTEGER,
                    request_payload JSONB
                );