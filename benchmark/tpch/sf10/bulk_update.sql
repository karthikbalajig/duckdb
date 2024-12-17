-- bulk_update.sql
BEGIN TRANSACTION;

-- Bulk Update 1: Increase customer balances for customers with more than 10 orders
UPDATE customer
SET c_acctbal = c_acctbal + 500
WHERE c_custkey IN (
    SELECT o_custkey
    FROM orders
    GROUP BY o_custkey
    HAVING COUNT(o_orderkey) > 10
);

-- Bulk Update 2: Apply a flat discount to all line items of orders in the last year
UPDATE lineitem
SET l_discount = l_discount + 0.02
WHERE l_orderkey IN (
    SELECT o_orderkey
    FROM orders
    WHERE o_orderdate >= DATE '1998-01-01'
);

-- Bulk Update 3: Mark old orders as archived
UPDATE orders
SET o_orderstatus = 'A' -- A for Archived
WHERE o_orderdate < DATE '1997-01-01';

-- Bulk Update 4: Adjust lineitem quantities for orders with large discounts
UPDATE lineitem
SET l_quantity = l_quantity + 5
WHERE l_orderkey IN (
    SELECT o_orderkey
    FROM orders
    WHERE o_totalprice < (
        SELECT AVG(o_totalprice)
        FROM orders
    )
);

COMMIT;
