
import pandas as pd
import numpy as np
import pandasql as ps
import random
import time
from datetime import datetime

orders_df   = pd.read_csv('loading data\order_data.csv')
customer_df   = pd.read_csv('loading data\customer_data.csv')
employee_df   = pd.read_csv('loading data\employee_data.csv')


customer_df

unique_province = customer_df[['province', 'province_code']].drop_duplicates()
province_df = pd.DataFrame(unique_province)
province_df['province_id'] = [*range(0,len(province_df))]
mien = ['Bắc' if i < 21 else 'Trung' if i < 42 else 'Nam' for i in province_df['province_id']]
for i in province_df['province_id']:
    province_df['Mien'] = mien

merged_df = pd.merge(customer_df, province_df, on='province', how='left')
province_ids = merged_df['province_id']
print(province_ids)

customer_df['province_id'] = province_ids

customer_df = customer_df.drop(['province', 'province_code'],axis=1)
# update the customer_df
customer_df.to_csv("loading data\customer_data.csv")
province_df

"""#Creating department_df and normalizing employee_df"""

companys = pd.Series(employee_df.company.unique()).to_list()
company_id = [*range(0, len(companys))]
company_df = pd.DataFrame(company_id, columns=['company_id'])
company_df['company'] = companys
company_df

merged1_df = pd.merge(employee_df, company_df, on='company', how='left')
company_ids = merged1_df['company_id']
print(company_ids)

employee_df['company_id'] = company_ids
employee_df

employee_df = employee_df.drop(['company'],axis=1)
# update the customer_df
employee_df.to_csv("loading data\employee_data.csv")

# save province and compan to csv
company_df.to_csv("loading data\company_data.csv")
province_df.to_csv("loading data\province_data.csv")