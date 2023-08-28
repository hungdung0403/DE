# -*- coding: utf-8 -*-
"""Transform.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1bi7NieaXqNh4xBjgJBGKT15ECb5k1DF3
"""

import pandas as pd
import numpy as np
import pandasql as ps
import random
import time
from datetime import datetime
from faker import Faker

"""### **Generating data**

#load product_df
"""

product_df = pd.read_csv('web_scraping\extracted_tiki_data.csv')
#product_df.head(5)

product_df = product_df.drop('Unnamed: 0', axis=1)
product_df = product_df.rename(columns = {'brand_name':'type'})

ptypes = ['Tiểu thuyết', 'Sách giáo khoa', 'Sách tự truyện', 'Sách tham khảo', 'Sách nấu ăn', 'Truyện tranh']
product_df['type'] = product_df['type'].fillna(pd.Series([random.choice(ptypes) for i in range(len(product_df))]))

id_len = len(str(product_df['id'].max()))
SKU_len = len(str(product_df['id'].max()))
product_df['id'] = product_df['id'].astype(str)
product_df['id'] = product_df['id'].str.zfill(id_len)
product_df['SKU'] = product_df['SKU'].astype(str)
product_df['SKU'] = product_df['SKU'].str.zfill(SKU_len)
def truncate_string(s):
    return s[:100]
product_df['name'] = product_df['name'].apply(truncate_string)


#product_df.head(5)
product_df.to_csv('loading data\product_data.csv')
"""#create Employee_df"""

employee_id = []
for i in range(100):
    x = random.randint(1, 999999)
    y = str(x).zfill(6)
    employee_id.append(y)
employee_id = list(set(employee_id))

employee_name = []
employee_city = []
etypes = ['Part-time', 'Full-time', 'Seasonal', 'Temporary', 'Leased']
employee_type = []
gender = ['male', 'female']
employee_gender = []
employee_company = []
employee_email = []

fake = Faker()
for i in range(len(employee_id)):
    employee_name.append(fake.name())
    employee_type.append(np.random.choice(etypes, 1)[0])
    employee_gender.append(np.random.choice(gender, 1)[0])
    employee_company.append(fake.company())
    employee_email.append(fake.email())

employee_df = pd.DataFrame(employee_id, columns = ['employee_id'])
employee_df['name'] = employee_name
employee_df['type'] = employee_type
employee_df['gender'] = employee_gender
employee_df['company'] = employee_company
employee_df['email'] = employee_email

employee_df['employee_id'] = employee_df['employee_id'].astype(str)
employee_df['employee_id'] = employee_df['employee_id'].str.zfill(6)
#employee_df.head(5)
employee_df.to_csv('loading data\employee_data.csv')
"""#Create Customer_df

"""

customer_id = []
for i in range(500):
    x = random.randint(1, 99999999)
    y = str(x).zfill(8)
    customer_id.append(y)
customer_id = list(set(customer_id))

customer_name = []
gender = ['male', 'female']
customer_gender = []
provinces = ["An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", "Bình Thuận", "Cà Mau", "Cao Bằng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang", "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang", "Hòa Bình", "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", "Ninh Bình","Ninh Thuận","Phú Thọ","Phú Yên","Quảng Bình","Quảng Nam","Quảng Ngãi","Quảng Ninh","Quảng Trị","Sóc Trăng","Sơn La","Tây Ninh","Thái Bình","Thái Nguyên","Thanh Hóa","Thừa Thiên Huế","Tiền Giang","Trà Vinh","Tuyên Quang","Vĩnh Long","Vĩnh Phúc","Yên Bái"]
customer_province = []
customer_email = []
types = ['Loyal', 'Impulse', 'Discount', 'Discount', 'Wandering']
customer_type = []
province_codes = []

for i in range(len(customer_id)):
    customer_name.append(fake.name())
    customer_gender.append(np.random.choice(gender, 1)[0])
    customer_province.append(np.random.choice(provinces, 1)[0])
    customer_type.append(np.random.choice(types, 1)[0])
    customer_email.append(fake.email())

customer_df = pd.DataFrame(customer_id, columns = ['customer_id'])
customer_df['name'] = customer_name
customer_df['province'] = customer_province
customer_df['type'] = customer_type
customer_df['gender'] = customer_gender
customer_df['email'] = customer_email
def abbreviate(s):
    words = s.split()
    return ''.join([word[0] for word in words])
customer_df['province_code'] = customer_df['province'].apply(abbreviate)

customer_df['customer_id'] = customer_df['customer_id'].astype(str)
customer_df['customer_id'] = customer_df['customer_id'].str.zfill(8)
#customer_df.head(5)
customer_df.to_csv("loading data\customer_data.csv")
"""#Create order_df"""

order_number = []
for i in range(50000):
    x = random.randint(1, 9999999999)
    y = str(x).zfill(10)
    order_number.append(y)
order_id = list(set(order_number))

order_quantity = []
order_date = []
order_cus_id = []
order_prod_id = []
order_staff_id = []
date_range = pd.date_range(start = "2020-01-01", end = "2023-12-31", freq="D")
for i in range(len(order_id)):
    order_quantity.append(np.random.choice(range(1, 50)))
    random_date = np.random.choice(date_range).astype('M8[D]').astype('O')  
    order_date.append(random_date.strftime('%Y-%m-%d'))
    order_cus_id.append(np.random.choice(customer_id, 1)[0])
    order_prod_id.append(np.random.choice(product_df['id'], 1)[0])
    order_staff_id.append(np.random.choice(employee_id, 1)[0])


order_df = pd.DataFrame(order_number, columns = ['order_number'])
order_df['customer_id'] = order_cus_id
order_df['product_id'] = order_prod_id
order_df['quantity'] = order_quantity
order_df['date'] = order_date
order_df['staff_id'] = order_staff_id

order_df['order_number'] = order_df['order_number'].astype(str)
order_df['order_number'] = order_df['order_number'].str.zfill(10)
#order_df.head(5)
order_df.to_csv("loading data\order_data.csv")