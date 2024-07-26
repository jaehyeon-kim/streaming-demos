--
-- Copy initial data records.
--

COPY customers(customer_id, company_name, country_code, phone)
FROM '/tmp/customers.csv'
DELIMITER ','
CSV HEADER;

COPY employees(employee_id, first_name, last_name)
FROM '/tmp/employees.csv'
DELIMITER ','
CSV HEADER;

COPY suppliers(supplier_id, company_name)
FROM '/tmp/suppliers.csv'
DELIMITER ','
CSV HEADER;

COPY products(product_id, supplier_id, product_name, unit_price, units_in_stock, units_on_order, discontinued)
FROM '/tmp/products.csv'
DELIMITER ','
CSV HEADER;
