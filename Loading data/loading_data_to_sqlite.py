import sqlite3
from os import curdir
import pandas as pd 
order_df   = pd.read_csv('loading data\order_data.csv',index_col=0)
customer_df   = pd.read_csv('loading data\customer_data.csv',index_col=0)
employee_df   = pd.read_csv('loading data\employee_data.csv',index_col=0)
product_df   = pd.read_csv('loading data\product_data.csv',index_col=0)
company_df   = pd.read_csv('loading data\company_data.csv',index_col=0)
province_df   = pd.read_csv('loading data\province_data.csv',index_col=0)

# rename
product_df = product_df.rename(columns={'id': 'product_id'})
order_df = order_df.rename(columns = {'staff_id':'employee_id'})

dataframes = [product_df, customer_df, employee_df, company_df, province_df, order_df,]

for df in dataframes:
    if "Unnamed: 0" in df.columns:
        df.drop("Unnamed: 0", axis=1, inplace=True)

db_path = r"loading data\retail_data.db"

retail_data = sqlite3.connect(db_path) #create book database

cur = retail_data.cursor() #create a Cursor object and call its execute() method to perform SQL queries


product_df.info()
cur.execute("DROP TABLE IF EXISTS PRODUCT")

tables_to_create = [
    "PRODUCT",
    "CUSTOMER",
    "EMPLOYEE",
    "COMPANY",
    "PROVINCE",
    "ORDERS"
]
for table_name in tables_to_create:
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")


#Tạo bảng PRODUCT
cur.execute('''CREATE TABLE PRODUCT
         (PRODUCT_ID           CHAR(9)      PRIMARY KEY     NOT NULL,
         SKU           CHAR(20),
         TYPE          VARCHAR(50),
         NAME          NVARCHAR(100),
         RATING_AVERAGE         FLOAT,
         PRICE        INT);''')

customer_df.info()

#Tạo bảng CUSTOMER
cur.execute('''CREATE TABLE CUSTOMER
         (CUSTOMER_ID          CHAR(8)      PRIMARY KEY     NOT NULL,
         NAME          VARCHAR(20),
         TYPE          VARCHAR(50),
         GENDER        VARCHAR(6),
         EMAIL         NVARCHAR(50),
         PROVINCE_ID         INT,
         FOREIGN KEY (PROVINCE_ID) REFERENCES PROVINCE(PROVINCE_ID));''')

employee_df.info()

#Tạo bảng EMPLOYEE

cur.execute('''CREATE TABLE EMPLOYEE
         (EMPLOYEE_ID          CHAR(6)      PRIMARY KEY     NOT NULL,
         NAME          VARCHAR(20),
         TYPE          VARCHAR(50),
         GENDER        VARCHAR(6),
         COMPANY_ID    INT,
         EMAIL         NVARCHAR(50),
         FOREIGN KEY (COMPANY_ID) REFERENCES COMPANY(COMPANY_ID));''')

province_df.info()

# Tạo bảng PROVINCE

cur.execute('''CREATE TABLE PROVINCE
         (PROVINCE_ID          INT      PRIMARY KEY     NOT NULL,
         PROVINCE          NVARCHAR(50),
         PROVINCE_CODE     NVARCHAR(6),
         MIEN        VARCHAR(5));''')

company_df.info()

# Tạo bảng COMPANY

cur.execute('''CREATE TABLE COMPANY
         (COMPANY_ID          INT      PRIMARY KEY     NOT NULL,
         COMPANY          VARCHAR(50));''')

order_df.info()

#Tạo bảng ORDER

cur.execute('''CREATE TABLE ORDERS
         (ORDER_NUMBER          CHAR(10)      PRIMARY KEY     NOT NULL,
         CUSTOMER_ID        CHAR(8),
         PRODUCT_ID         CHAR(9),
         EMPLOYEE_ID        CHAR(6),
         DATE               DATETIME,
         QUANTITY           INT,
         FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER(CUSTOMER_ID),
         FOREIGN KEY (EMPLOYEE_ID) REFERENCES EMPLOYEE(EMPLOYEE_ID),
         FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID)
    );''')

#remove duplicated data 

for table_name, df in zip(tables_to_create, dataframes):
    existing_data = pd.read_sql(f'SELECT * FROM {table_name}', retail_data)
    df = df[~df.apply(tuple,1).isin(existing_data.apply(tuple,1))]

# Insert data từ dataframe qua database
for df, table_name in zip(dataframes, tables_to_create):
    df.to_sql(f'{table_name}', retail_data, if_exists='append', index=False)


# Chạy thử
for row in cur.execute("SELECT ORDER_NUMBER, PRODUCT_ID, DATE, QUANTITY FROM ORDERS ORDER BY DATE LIMIT 5"):
    print("ORDER_ID = ", row[0])
    print("PRODUCT_ID = ", row[1])
    print("DATE = ", row[2])
    print("QUANTITY = ", row[3], "\n")

for row in cur.execute('''
SELECT O.ORDER_NUMBER, O.PRODUCT_ID, O.DATE, O.QUANTITY
FROM ORDERS O
JOIN PRODUCT P ON O.PRODUCT_ID = P.PRODUCT_ID
ORDER BY O.DATE
LIMIT 5
'''):
    print("ORDER_ID = ", row[0])
    print("PRODUCT_ID = ", row[1])
    print("DATE = ", row[2])
    print("QUANTITY = ", row[3], "\n")

# Lưu thao tác và đóng con trỏ giải phóng tài nguyên
retail_data.commit()
cur.close()
retail_data.close()
