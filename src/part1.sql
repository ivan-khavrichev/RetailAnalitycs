-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

-- CREATE DATABASE Retail_Analytics;


DROP TABLE IF EXISTS Date_of_Analysis;
DROP TABLE IF EXISTS Stores;
DROP TABLE IF EXISTS Checks;
DROP TABLE IF EXISTS Product_Grid;
DROP TABLE IF EXISTS SKU_Group;
DROP TABLE IF EXISTS Transactions;
DROP TABLE IF EXISTS Cards;
DROP TABLE IF EXISTS Personal_Information;

-- Экспорт csv
CREATE OR REPLACE PROCEDURE export_table(IN name_table VARCHAR, IN file_path VARCHAR, IN separ VARCHAR) AS $$
DECLARE
    copy_command VARCHAR;
BEGIN
    copy_command = 'COPY ' || name_table || ' TO ' || quote_literal(file_path) || ' DELIMIT' || 'ER' || quote_literal(separ) || ' TSV HEADER';
    EXECUTE copy_command; 
END;
$$ LANGUAGE plpgsql;

-- Импорт csv
CREATE OR REPLACE PROCEDURE import_table(IN name_table VARCHAR, IN file_path VARCHAR, IN separ VARCHAR) AS $$
DECLARE
    copy_command VARCHAR;
    del_command VARCHAR;
BEGIN
    del_command = 'DELETE FROM ' || name_table;
    EXECUTE del_command;
    copy_command = 'COPY ' || name_table || ' FROM ''' || file_path || ''' DELIMIT' || 'ER E''' || separ || '''';
    EXECUTE copy_command; 
END;
$$ LANGUAGE plpgsql;

-- Создение таблиц (добавить ограничения по формату и внешние ключи)

CREATE TABLE IF NOT EXISTS Personal_Information (
  Customer_ID INT PRIMARY KEY NOT NULL,
  Customer_Name VARCHAR NOT NULL CHECK (Customer_Name ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
  Customer_Surname VARCHAR NOT NULL CHECK  (Customer_Surname ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
  Customer_Primary_Email VARCHAR NOT NULL CHECK  (Customer_Primary_Email ~ '^[-\w\.]+@([\w]+\.)+[\w]{2,4}$'),
  Customer_Primary_Phone  VARCHAR NOT NULL CHECK (Customer_Primary_Phone ~ '^\+7\d{10}$')
);

CREATE TABLE IF NOT EXISTS Cards (
  Customer_Card_ID INT PRIMARY KEY NOT NULL,
  Customer_ID INT NOT NULL,
  CONSTRAINT fk_Customer_ID
  FOREIGN KEY (Customer_ID) REFERENCES Personal_Information(Customer_ID)
  ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS Transactions (
  Transaction_ID INT PRIMARY KEY NOT NULL,
  Customer_Card_ID INT NOT NULL,
  Transaction_Summ FLOAT NOT NULL,
  Transaction_DateTime TIMESTAMP NOT NULL,
  Transaction_Store_ID INT NOT NULL,
  CONSTRAINT fk_Customer_Card
  FOREIGN KEY (Customer_Card_ID) REFERENCES Cards(Customer_Card_ID) 
  ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS SKU_Group (
  Group_ID INT PRIMARY KEY NOT NULL,
  Group_Name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS Product_Grid (
  SKU_ID INT PRIMARY KEY NOT NULL,
  SKU_Name VARCHAR NOT NULL,
  Group_ID INT NOT NULL,
  CONSTRAINT fk_Group_ID
  FOREIGN KEY (Group_ID) REFERENCES SKU_Group(Group_ID) 
  ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS Checks (
  Transaction_ID INT NOT NULL,
  SKU_ID INT NOT NULL,
  SKU_Amount FLOAT NOT NULL,
  SKU_Summ FLOAT NOT NULL,
  SKU_Summ_Paid FLOAT NOT NULL,
  SKU_Discount FLOAT NOT NULL,
  CONSTRAINT fk_Transaction_ID 
  FOREIGN KEY (Transaction_ID) REFERENCES Transactions(Transaction_ID) 
  ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_SKU_ID
  FOREIGN KEY (SKU_ID) REFERENCES Product_Grid(SKU_ID) 
  ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS Stores (
  Transaction_Store_ID INT NOT NULL,
  SKU_ID INT NOT NULL,
  SKU_Purchase_Price FLOAT NOT NULL,
  SKU_Retail_Price FLOAT NOT NULL,
  CONSTRAINT fk_SKU_ID
  FOREIGN KEY (SKU_ID) REFERENCES Product_Grid(SKU_ID) 
  ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS Date_of_Analysis (
  Analysis_Formation TIMESTAMP
);

SET datestyle = "european";

DROP INDEX IF EXISTS idx_checks_sku_id;
DROP INDEX IF EXISTS idx_checks_transaction_id;
DROP INDEX IF EXISTS idx_personal_information_customer_id;
DROP INDEX IF EXISTS idx_groups_sku_group_id;
DROP INDEX IF EXISTS  idx_product_grid_sku_id;
DROP INDEX IF EXISTS idx_cards_customer_card_id;
DROP INDEX IF EXISTS idx_cards_customer_id;
DROP INDEX IF EXISTS idx_transactions_customer_card_id;
DROP INDEX IF EXISTS idx_transactions_transaction_id;
DROP INDEX IF EXISTS idx_stores_sku_id;

CREATE UNIQUE INDEX idx_personal_information_customer_id ON Personal_information USING btree(customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cards_customer_card_id ON Cards USING btree(Customer_Card_ID);
CREATE INDEX IF NOT EXISTS idx_cards_customer_id ON Cards USING btree(Customer_ID);
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_transaction_id ON Transactions USING btree(Transaction_ID);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_card_id ON Transactions USING btree(Customer_Card_ID);
CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_sku_group_id ON SKU_Group USING btree(Group_ID);
CREATE UNIQUE INDEX IF NOT EXISTS idx_product_grid_sku_id ON Product_Grid USING btree(SKU_ID);
CREATE INDEX IF NOT EXISTS idx_stores_sku_id ON Stores USING btree(SKU_ID);
CREATE INDEX IF NOT EXISTS idx_checks_transaction_id ON Checks USING btree(Transaction_ID);
CREATE INDEX IF NOT EXISTS idx_checks_sku_id ON Checks USING btree(SKU_ID);

CALL import_table(name_table:='Date_of_Analysis', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Date_Of_Analysis_Formation.tsv', separ:='\t');
CALL import_table(name_table:='Personal_Information', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Personal_Data_Mini.tsv', separ:='\t');
CALL import_table(name_table:='Cards', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Cards_Mini.tsv', separ:='\t');
CALL import_table(name_table:='Transactions', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Transactions_Mini.tsv', separ:='\t');
CALL import_table(name_table:='SKU_Group', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Groups_SKU_Mini.tsv', separ:='\t');
CALL import_table(name_table:='Product_Grid', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/SKU_Mini.tsv', separ:='\t');
CALL import_table(name_table:='Stores', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Stores_Mini.tsv', separ:='\t');
CALL import_table(name_table:='Checks', file_path:='/Users/ivan-khavricev/code/school21/sql/SQL3_RetailAnalitycs_v1.0-1/src/datasets/Checks_Mini.tsv', separ:='\t');
