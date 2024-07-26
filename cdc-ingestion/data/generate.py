from faker import Faker

from models import Customer, Employee, Product, Order
from dbhelper import DbHelper

if __name__ == "__main__":
    fake = Faker()
    Faker.seed(1237)
    db = DbHelper()
    customers = Customer.fetch(db)
    products = Product.fetch(db)
    employees = Employee.fetch(db)
    for i in range(3):
        order = Order.generate(fake, customers, products, employees)
        print(order)
