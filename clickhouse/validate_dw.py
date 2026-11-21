import base64
import os
import urllib.parse
import urllib.request


credentials = (
    f'{os.environ["CH_USER"]}:{os.environ["CH_PASSWORD"]}'
).encode("utf-8")

authorization = "Basic " + base64.b64encode(credentials).decode("ascii")

BASE_URL = "http://chinook-clickhouse:8123/"


def query_number(sql):
    url = BASE_URL + "?" + urllib.parse.urlencode({"query": sql})

    request = urllib.request.Request(
        url,
        headers={"Authorization": authorization},
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        return int(response.read().decode("utf-8").strip())


checks = {
    "CUSTOMERS_WITH_INVALID_CURRENT_COUNT": """
        SELECT count()
        FROM (
            SELECT CustomerId
            FROM ChinookDW.DimCustomer
            GROUP BY CustomerId
            HAVING countIf(IsCurrent = 1) != 1
        )
    """,

    "DUPLICATE_CUSTOMER_KEYS": """
        SELECT count()
        FROM (
            SELECT CustomerKey
            FROM ChinookDW.DimCustomer
            GROUP BY CustomerKey
            HAVING count() > 1
        )
    """,

    "INVALID_SCD2_DATES": """
        SELECT count()
        FROM ChinookDW.DimCustomer
        WHERE
            (IsCurrent = 1 AND ValidTo IS NOT NULL)
            OR
            (IsCurrent = 0 AND
                (ValidTo IS NULL OR ValidTo < ValidFrom))
    """,

    "DUPLICATE_INVOICE_LINES": """
        SELECT count()
        FROM (
            SELECT InvoiceLineId
            FROM ChinookDW.FactSales
            GROUP BY InvoiceLineId
            HAVING count() > 1
        )
    """,

    "ORPHAN_SALES": """
        SELECT count()
        FROM ChinookDW.FactSales
        WHERE CustomerKey NOT IN (
            SELECT CustomerKey
            FROM ChinookDW.DimCustomer
        )
    """,

    "INVALID_SALES_AMOUNTS": """
        SELECT count()
        FROM ChinookDW.FactSales
        WHERE abs(
            toFloat64(SalesAmount)
            - toFloat64(UnitPrice) * toFloat64(Quantity)
        ) > 0.005
    """,
}

print("=== CLICKHOUSE DATA QUALITY ===", flush=True)

failures = []

for name, sql in checks.items():
    count = query_number(sql)
    status = "PASS" if count == 0 else "FAIL"

    print(f"{name}: {count} [{status}]", flush=True)

    if count != 0:
        failures.append(name)

if failures:
    raise SystemExit(
        "CLICKHOUSE_DATA_QUALITY_FAILED: " + ", ".join(failures)
    )

print("CLICKHOUSE_DATA_QUALITY_OK", flush=True)
