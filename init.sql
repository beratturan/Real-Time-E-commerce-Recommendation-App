CREATE TABLE Views (
    event VARCHAR(100),
    messageid VARCHAR(100),
    user_id VARCHAR(100),
    productid VARCHAR(100),
    source VARCHAR(100),
    time_stamp VARCHAR(100)
);

CREATE TABLE non_pers_bestseller (
    productid VARCHAR(100),
    user_count INT,
    product_count INT
);

