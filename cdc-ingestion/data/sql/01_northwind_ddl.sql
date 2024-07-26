--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET default_tablespace = '';
SET default_with_oids = false;

--
-- Name: customers; Type: TABLE; 
--

CREATE TABLE IF NOT EXISTS customers (
    customer_id uuid DEFAULT gen_random_uuid(),
    company_name character varying(40) NOT NULL,
    country_code character varying(2),
    phone character varying(24),
    PRIMARY KEY (customer_id)
);

--
-- Name: employees; Type: TABLE; 
--

CREATE TABLE IF NOT EXISTS employees (
    employee_id uuid DEFAULT gen_random_uuid(),
    first_name character varying(20) NOT NULL,
    last_name character varying(20) NOT NULL,
    PRIMARY KEY (employee_id)
);

--
-- Name: suppliers; Type: TABLE; 
--

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id uuid DEFAULT gen_random_uuid(),
    company_name character varying(40) NOT NULL,
    PRIMARY KEY (supplier_id)
);

--
-- Name: products; Type: TABLE;
--

CREATE TABLE IF NOT EXISTS products (
    product_id uuid DEFAULT gen_random_uuid(),
    supplier_id uuid NOT NULL,
    product_name character varying(40) NOT NULL,
    unit_price real,
    units_in_stock smallint,
    units_on_order smallint,
    discontinued integer NOT NULL,
    PRIMARY KEY (product_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers
);

--
-- Name: orders; Type: TABLE; 
--

CREATE TABLE IF NOT EXISTS orders (
    order_id uuid DEFAULT gen_random_uuid(),
    customer_id uuid,
    employee_id uuid,
    order_date date,
    required_date date,
    shipped_date date,
    freight real,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers,
    FOREIGN KEY (employee_id) REFERENCES employees
);

--
-- Name: order_details; Type: TABLE; 
--

CREATE TABLE IF NOT EXISTS order_details (
    order_id uuid NOT NULL,
    product_id uuid NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (product_id) REFERENCES products,
    FOREIGN KEY (order_id) REFERENCES orders
);
