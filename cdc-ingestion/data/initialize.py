import os
import shutil
import csv
import dataclasses
from faker import Faker


@dataclasses.dataclass
class Supplier:
    supplier_id: str
    company_name: str

    @classmethod
    def generate(cls, fake: Faker):
        return cls(supplier_id=fake.uuid4(), company_name=fake.company())


@dataclasses.dataclass
class Customer:
    customer_id: str
    customer_name: str
    country: str
    phone: str

    @classmethod
    def generate(cls, fake: Faker):
        return cls(
            customer_id=fake.uuid4(),
            customer_name=fake.company(),
            country=fake.country(),
            phone=fake.basic_phone_number(),
        )


def wirte_to_csv(mapping: list, src_path: str):
    fake = Faker()
    Faker.seed(1237)
    for obj in mapping:
        items = [
            dataclasses.asdict(obj["object"].generate(fake)) for _ in range(obj["num"])
        ]
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
        {"name": "supplier", "object": Supplier, "num": 30},
        {"name": "customer", "object": Customer, "num": 100},
    ]
    wirte_to_csv(object_mapping, src_path)
