import requests
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

### Login

login_url= 'https://app.sarvcrm.com/API.php?method=Login'
username= 's.salehin'
password= '38aa7a9ee7d859e053beb13f43e4305a'
utype= 'persol'

login_data= {
    'utype' : utype,
    'username' : username,
    'password' : password
}

login_response= requests.post(login_url, data= login_data, verify= False)
if login_response.status_code == 200:
    login_data = login_response.json()
    api_token = login_data.get('data')['token']
else:
    print("API token not found in the login response")

### Leads

api_url= 'https://app.sarvcrm.com/API.php?method=Retrieve&module=Leads'
headers = {
    'Authorization': f'Bearer {api_token}'
}

payload = {'limit': '200'}

response = requests.post(api_url, data= payload ,headers=headers, verify= False)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON data returned by the API
    data = response.json()
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")

df_lead= pd.DataFrame(data['data'])

### Accounts

api_url= 'https://app.sarvcrm.com/API.php?method=Retrieve&module=Accounts'

headers = {
    'Authorization': f'Bearer {api_token}'
}

response = requests.post(api_url, data= payload ,headers=headers, verify= False)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON data returned by the API
    data = response.json()
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")

df_accounts= pd.DataFrame(data['data'])

### Opportunities

api_url= 'https://app.sarvcrm.com/API.php?method=Retrieve&module=Opportunities'

headers = {
    'Authorization': f'Bearer {api_token}'
}

response = requests.post(api_url, data= payload ,headers=headers, verify= False)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON data returned by the API
    data = response.json()
    print(data)
else:
    print(f"Request failed with status code {response.status_code}")

df_opportunities= pd.DataFrame(data['data'])


server = '192.168.10.41'
database = 'Marketing_DataMart'
username = 'SelfServiceBI'
password = 'MobbMobb66!'


# Write DataFrames to SQL Server
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')

# Write DataFrames to SQL Server
# leads
table_name = 'Leads'
df_lead['date_entered']= df_lead['date_entered'].apply(lambda x: x[:10])
df_lead= df_lead[[
    'date_entered',
    'id',
    'assigned_user_name',
    'persol_lead_businesscategory',
    'account_id',
    'opportunity_id',
    'primary_number_raw',
    'status',
    'lead_source',
    'full_name',
    'primary_address_city',
    'account_name',
    'campaign_name'
]]
df_lead= df_lead.astype(str)
existing_ids = pd.read_sql(f"SELECT id FROM {table_name}", con=engine)['id'].tolist()
df = df_lead[~df_lead['id'].isin(existing_ids)]
df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

# accounts
table_name = 'Accounts'
df_accounts['date_entered']= df_accounts['date_entered'].apply(lambda x: x[:10])
df_accounts= df_accounts[[
    'date_entered',
    'id',
    'assigned_user_name',
    'name',
    'billing_address_city',
    'primary_number_raw',
    'campaign_name'
]]
df_accounts= df_accounts.astype(str)
existing_ids = pd.read_sql(f"SELECT id FROM {table_name}", con=engine)['id'].tolist()
df = df_accounts[~df_accounts['id'].isin(existing_ids)]
df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

# opportunities
table_name = 'Opportunities'
df_opportunities['date_entered']= df_opportunities['date_entered'].apply(lambda x: x[:10])
df_opportunities= df_opportunities[[
    'date_entered',
    'id',
    'assigned_user_name',
    'account_name',
    'date_closed',
    'campaign_name',
    'sales_stage',
    'probability'
]]
df_opportunities= df_opportunities.astype(str)
existing_ids = pd.read_sql(f"SELECT id FROM {table_name}", con=engine)['id'].tolist()
df = df_opportunities[~df_opportunities['id'].isin(existing_ids)]
df.to_sql(name=table_name, con=engine, index=False, if_exists='append')