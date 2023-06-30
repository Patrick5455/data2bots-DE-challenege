def crate_staging_tables(schema_name: str, table_name: str) -> str:
    print(f"selecting query to create {table_name} table")
    if table_name == "reviews":
        return f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        review INT NOT NULL,
        product_id INT NOT NULL
--         CONSTRAINT fk_product_review
--             FOREIGN KEY (product_id)
--                 REFERENCES if_common.dim_products(product_id)
        );
        """

    if table_name == "orders":
        return f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ( 
            order_id INT NOT NULL,
            customer_id INT NOT NULL,
            order_date DATE NOT NULL,
            product_id INT NOT NULL,
            unit_price INT NOT NULL,
            quantity INT NOT NULL,
            total_price INT NOT NULL,
            PRIMARY KEY (order_id)
--             CONSTRAINT fk_customer
--                 FOREIGN KEY(customer_id)
--                     REFERENCES if_common.dim_customers(customer_id)
--          CONSTRAINT fk_product_order
--              FOREIGN KEY (product_id)
--                  REFERENCES if_common.dim_products(product_id)
        );
        """

    if table_name == "shipment_deliveries":
        return f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}  (
        shipment_id INT NOT NULL,
        order_id INT  NOT NULL,
        shipment_date DATE  NULL,
        delivery_date DATE,
        PRIMARY KEY (shipment_id)
--        CONSTRAINT fk_order_shipment_delivery
--            FOREIGN KEY(order_id)
--                REFERENCES {schema_name}.orders(order_id)
        );
        """
