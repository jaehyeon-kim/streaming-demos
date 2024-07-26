import os
import shutil
import csv
import dataclasses
from faker import Faker

from models import Customer, Employee, Supplier, Product


def wirte_to_csv(mapping: list, src_path: str):
    fake = Faker()
    Faker.seed(1237)
    suppliers = None
    for obj in mapping:
        if obj["name"] == "products":
            items = [
                dataclasses.asdict(
                    obj["object"].generate(fake, [s["supplier_id"] for s in suppliers])
                )
                for _ in range(obj["num"])
            ]
        else:
            items = [
                dataclasses.asdict(obj["object"].generate(fake))
                for _ in range(obj["num"])
            ]
            if obj["name"] == "suppliers":
                suppliers = items
        with open(os.path.join(src_path, "csvs", f"{obj['name']}.csv"), "w") as f:
            w = csv.DictWriter(f, items[0].keys())
            w.writeheader()
            w.writerows(items)


if __name__ == "__main__":
    src_path = os.path.dirname(os.path.realpath(__file__))
    print("recreate csvs folder")
    shutil.rmtree(os.path.join(src_path, "csvs"), ignore_errors=True)
    os.mkdir(os.path.join(src_path, "csvs"))
    print("write initial data sets")
    object_mapping = [
        {"name": "customers", "object": Customer, "num": 200},
        {"name": "employees", "object": Employee, "num": 100},
        {"name": "suppliers", "object": Supplier, "num": 60},
        # product requires supplier ids, should come after supplier
        {"name": "products", "object": Product, "num": 100},
    ]
    wirte_to_csv(object_mapping, src_path)
