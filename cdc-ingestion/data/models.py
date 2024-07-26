import dataclasses
import string
import typing
from datetime import date, datetime, timedelta

from faker import Faker
from dbhelper import DbHelper


@dataclasses.dataclass
class Customer:
    customer_id: str
    company_name: str
    country_code: str
    phone: str

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(cls, fake: Faker):
        return cls(
            customer_id=fake.uuid4(),
            company_name=fake.company(),
            country_code=fake.country_code(),
            phone=fake.basic_phone_number(),
        )

    @staticmethod
    def fetch(db: DbHelper):
        stmt = "SELECT customer_id, company_name, country_code, phone FROM customers"
        return [Customer.from_json(r) for r in db.fetch_records(stmt)]


@dataclasses.dataclass
class Employee:
    employee_id: str
    first_name: str
    last_name: str

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(cls, fake: Faker):
        return cls(
            employee_id=fake.uuid4(),
            first_name=fake.first_name(),
            last_name=fake.last_name(),
        )

    @staticmethod
    def fetch(db: DbHelper):
        stmt = "SELECT employee_id, first_name, last_name FROM employees"
        return [Employee.from_json(r) for r in db.fetch_records(stmt)]


@dataclasses.dataclass
class Supplier:
    supplier_id: str
    company_name: str

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(cls, fake: Faker):
        return cls(supplier_id=fake.uuid4(), company_name=fake.company())

    @staticmethod
    def fetch(db: DbHelper):
        stmt = "SELECT supplier_id, company_name FROM suppliers"
        return [Supplier.from_json(r) for r in db.fetch_records(stmt)]


@dataclasses.dataclass
class Product:
    product_id: str
    supplier_id: str
    product_name: str
    unit_price: float
    units_in_stock: int
    units_on_order: int
    units_on_order: int

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(cls, fake: Faker, supplier_ids: list):
        return cls(
            product_id=fake.uuid4(),
            supplier_id=supplier_ids[
                fake.pyint(min_value=0, max_value=len(supplier_ids) - 1)
            ],
            product_name=fake.pystr_format(
                "????-??????{{random_letter}}", letters=string.ascii_uppercase
            ),
            unit_price=fake.pyfloat(left_digits=3, right_digits=2, positive=True),
            units_in_stock=fake.pyint(min_value=10, max_value=1000),
            units_on_order=fake.pyint(min_value=0, max_value=100),
            discontinued=0,
        )

    @staticmethod
    def fetch(db: DbHelper):
        stmt = "SELECT product_id, supplier_id, product_name, unit_price, units_in_stock, units_on_order, units_on_order FROM products"
        return [Product.from_json(r) for r in db.fetch_records(stmt)]


@dataclasses.dataclass
class OrderDetail:
    order_id: str
    product_id: str
    quantity: int
    discount: float

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(cls, fake: Faker, order_id: str, product_id: str):
        return cls(
            order_id=order_id,
            product_id=product_id,
            quantity=fake.pyint(min_value=1, max_value=5),
            discount=fake.pyfloat(min_value=0, max_value=0.4, right_digits=2),
        )


@dataclasses.dataclass
class Order:
    order_id: str
    customer_id: str
    employee_id: str
    order_date: date
    required_date: date
    shipped_date: date
    freight: float
    order_details: typing.List[OrderDetail]
    created_at: datetime

    @classmethod
    def from_json(cls, r: dict):
        return cls(**r)

    @classmethod
    def generate(
        cls,
        fake: Faker,
        customers: typing.List[Customer],
        products: typing.List[Product],
        employees: typing.List[Employee],
    ):
        order_id = fake.uuid4()
        created_at = datetime.now() + timedelta(
            seconds=fake.pyint(min_value=-60, max_value=0)
        )
        order_details = [
            OrderDetail.generate(
                fake,
                order_id,
                products[
                    fake.pyint(min_value=0, max_value=len(products) - 1)
                ].product_id,
            )
            for _ in range(fake.pyint(min_value=1, max_value=5))
        ]

        return cls(
            order_id=order_id,
            customer_id=[e.customer_id for e in customers][
                fake.pyint(min_value=0, max_value=len(customers) - 1)
            ],
            employee_id=[e.employee_id for e in employees][
                fake.pyint(min_value=0, max_value=len(employees) - 1)
            ],
            order_date=created_at.date(),
            required_date=created_at.date() + timedelta(days=3),
            shipped_date=created_at.date() + timedelta(days=1),
            freight=fake.pyfloat(left_digits=2, right_digits=2, positive=True),
            order_details=order_details,
            created_at=created_at,
        )
