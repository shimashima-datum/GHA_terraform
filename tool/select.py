#!/usr/bin/env python3
import os
import csv
import sys
from typing import Optional
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def getenv_required(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        print(f"[ERROR] Required env var not set: {key}", file=sys.stderr)
        sys.exit(1)
    return v

def load_private_key(pem_path: str):
    with open(pem_path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def main():
    # 必須
    account   = getenv_required("SNOWFLAKE_ACCOUNT")
    user      = getenv_required("SNOWFLAKE_USER")
    warehouse = getenv_required("SNOWFLAKE_WAREHOUSE")
    database  = getenv_required("SNOWFLAKE_DATABASE")
    key_path  = getenv_required("SNOWFLAKE_PRIVATE_KEY_PATH")

    # 任意
    role: Optional[str] = os.environ.get("SNOWFLAKE_ROLE") or None
    schema_like = os.environ.get("SNOWFLAKE_SCHEMA_LIKE") or "%"

    private_key = load_private_key(key_path)

    print(f"[INFO] Connecting to Snowflake account={account}, user={user}, role={role}, wh={warehouse}, db={database}, schema_like={schema_like}")
    ctx = snowflake.connector.connect(
        account=account,
        user=user,
        private_key=private_key,
        role=role,
        warehouse=warehouse,
        database=database,
    )
    cs = ctx.cursor()
    try:
        q = """
            select table_schema, table_name, table_type
            from information_schema.tables
            where table_catalog = %s
              and table_schema like %s
            order by 1,2
        """
        cs.execute(q, (database, schema_like))
        rows = cs.fetchall()

        for s, t, ty in rows:
            print(f"{s}.{t}  ({ty})")

        with open("tables.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["table_schema", "table_name", "table_type"])
            w.writerows(rows)

        print(f"[INFO] Saved: tables.csv ({len(rows)} rows)")
    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    main()
