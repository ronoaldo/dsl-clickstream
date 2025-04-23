"""
Module schemas holds the data layout to be stored in Bigquery.
This makes easier to use create load jobs for the various data formats.
"""

# Bigquery Schemas
RAW = {
    "fields": [
        {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "geolocation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "events", "type": "RECORD", "mode": "REPEATED",
            "fields": [
                {"name": "event", "type": "RECORD", "mode": "NULLABLE",
                     "fields": [
                        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
                        {"name": "details", "type": "RECORD", "mode": "NULLABLE",
                             "fields": [
                                 {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "referrer_url", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
                                 {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                                 {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
                                 {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
                                 {"name": "items", "type":"RECORD", "mode": "REPEATED",
                                      "fields": [
                                          {"name":"product_id", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"product_name", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"category", "type":"STRING", "mode": "NULLABLE"},
                                          {"name":"price", "type":"FLOAT", "mode": "NULLABLE"},
                                          {"name":"quantity", "type":"INTEGER", "mode": "NULLABLE"},
                                      ],
                                 },
                             ],
                        },
                     ],
                },
            ],
        },
    ],
}

SESSION = {
    "fields": [
        {"name": "session_key","type": "STRING","mode": "NULLABLE"},
        {"name": "session_id","type": "STRING","mode": "NULLABLE"},
        {"name": "user_id","type": "STRING","mode": "NULLABLE" },
        {"name": "user_agent","type": "STRING","mode": "NULLABLE"},
        {"name": "geolocation", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_type","type": "STRING","mode": "NULLABLE"},
    ]
}

PAGEVIEW = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "page_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referrer_url", "type": "STRING", "mode": "NULLABLE"},
    ]
}

ADDTOCART = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}

PURCHASE = {
    "fields": [
        {"name": "session_key", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "items", "type": "RECORD", "mode": "REPEATED",
            "fields": [
                {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "product_id", "type": "STRING", "mode": "NULLABLE"}
            ]
        },
    ]
}
