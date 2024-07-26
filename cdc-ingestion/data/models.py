import dataclasses
import string

from faker import Faker


@dataclasses.dataclass
class Customer:
    customer_id: str
    customer_name: str
    country_code: str
    phone: str

    @classmethod
    def generate(cls, fake: Faker):
        return cls(
            customer_id=fake.uuid4(),
            customer_name=fake.company(),
            country_code=fake.country_code(),
            phone=fake.basic_phone_number(),
        )


@dataclasses.dataclass
class Employee:
    employee_id: str
    first_name: str
    last_name: str

    @classmethod
    def generate(cls, fake: Faker):
        return cls(
            employee_id=fake.uuid4(),
            first_name=fake.first_name(),
            last_name=fake.last_name(),
        )


@dataclasses.dataclass
class Supplier:
    supplier_id: str
    company_name: str

    @classmethod
    def generate(cls, fake: Faker):
        return cls(supplier_id=fake.uuid4(), company_name=fake.company())


@dataclasses.dataclass
class Product:
    product_id: str
    supplier_id: str
    product_name: str
    unit_price: float
    units_in_stock: int
    units_on_order: int
    discontinued: int

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
