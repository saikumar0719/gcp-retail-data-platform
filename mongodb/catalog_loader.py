"""Product catalog loader -> MongoDB.

Schema decision (documented, interview-ready):
- Product core attributes are EMBEDDED in one document: a product is read
  whole almost every time, so one read returns everything.
- Price history is REFERENCED (separate collection): it grows unboundedly
  and is rarely read with the product, so embedding it would bloat docs.

    python mongodb/catalog_loader.py --uri mongodb://retail:retailpass@localhost:27017
"""

import argparse
import json
import random

from pymongo import MongoClient

CATEGORIES = ["apparel", "footwear", "accessories", "home"]


def build_catalog(n: int = 200) -> list:
    catalog = []
    for i in range(1, n + 1):
        catalog.append(
            {
                "_id": f"SKU-{i:04d}",
                "name": f"Product {i}",
                "category": random.choice(CATEGORIES),
                "attributes": {
                    "sizes": ["S", "M", "L", "XL"],
                    "colors": ["black", "white", "navy"],
                },
                "current_price": round(random.uniform(9.99, 299.99), 2),
            }
        )
    return catalog


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="mongodb://retail:retailpass@localhost:27017")
    parser.add_argument("--db", default="retail")
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]
    products = db["products"]
    products.delete_many({})
    result = products.insert_many(build_catalog())
    print(f"Loaded {len(result.inserted_ids)} products into {args.db}.products")


if __name__ == "__main__":
    main()
