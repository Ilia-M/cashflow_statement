import pandas as pd
import plaid
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials

# Api keys для аккаунта на plaid.com:
PLAID_CLIENT_ID = '5ecbce6555135b0011c770fc'
PLAID_SECRET = '9317ae25b1a5073841b97aaee7abe2'
PLAID_PUBLIC_KEY = '56970fd93966a27388664560baf329'
PLAID_PRODUCTS = 'transactions'
PLAID_COUNTRY_CODES = 'US'
PLAID_ENV ='sandbox'

# Токен доступа для Plaid Quickstart:
access_token = 'access-sandbox-bdcb4e40-d464-4323-9e1d-b301cded84d6'

# Ключ для Google API:
CREDENTIALS_FILE = r'E:\Projects\Interview\xO Analytics\My Project 7180-40a1670deda1.json'

# Подключение к Google Sheets:
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets',
                                                                                  'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)
spreadsheetId = '1Qf4l5H-688wYqGuLQGbgg04-_VZstc2pT-3XLcicgmU'

# Загрузка из Google Sheets периода отчета:
range_name = 'cashflow_statement!B1:B2'
var_table = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_name).execute()
s_date = ""
s_date = s_date.join(var_table['values'][0]) # Конвертация списка в строку
e_date = ""
e_date = e_date.join(var_table['values'][1])

# Запрос для загрузки транзакций c сервера Plaid Quickstart:
client = plaid.Client(client_id = PLAID_CLIENT_ID, secret=PLAID_SECRET,
                      public_key=PLAID_PUBLIC_KEY, environment=PLAID_ENV, api_version='2019-05-29')
response = client.Transactions.get(access_token,
                                   start_date=s_date,
                                   end_date=e_date)
transactions = pd.DataFrame(response['transactions'])

# Выгрузка полученных данных в Google Sheets(transactions):
transactions_out = transactions.astype(str)
a = transactions_out.columns.values.tolist()
b = transactions_out.values.tolist()
b.insert(0, a)
results = service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, body = {
    "valueInputOption": "USER_ENTERED",
    "data": [
        {"range": "transactions!A1:z2000",
         "majorDimension": "ROWS",     
         "values": b}
            ]
}).execute()

# Формирование cashflow-отчета:
transactions[['Category 1','Category 2', 'Category 3']] = pd.DataFrame(transactions.category.tolist(), index = transactions.index)
transactions['month'] = transactions['date'].astype('datetime64[M]')

def am_type (row): # функция возвращает тип транзакции в зависимости от знака 
    if row['amount'] < 0:
        return 'Income'
    else:
        return 'Expense'
transactions['amount_type'] = transactions.apply(am_type, axis=1)

transactions.fillna('null', inplace = True)
type_list = transactions['amount_type'].unique()
cat1_list = transactions['Category 1'].unique()
cat2_list = transactions['Category 2'].unique()
cat3_list = transactions['Category 3'].unique()
transactions = transactions[['amount', 'date', 'month', 'Category 1','Category 2', 'Category 3', 'amount_type']]
final_size = transactions.pivot_table(index=['amount_type','Category 1', 'Category 2', 'Category 3'],
                                      columns = 'month', values='amount', aggfunc='sum')
final_zero = pd.DataFrame(index=final_size.index, columns=final_size.columns)#.reset_index()                                          
final_zero['grand total'] = final_zero.apply(lambda _: '', axis=1) # пустой датафрейм с требуемым индексом
final_zero.dropna(inplace=True) # пустой датафрейм с необходимыми столбцами
final = final_zero
for i in range(len(type_list)):                 
    final_cat1 = final_zero
    for j in range(len(cat1_list)):             
        final_cat2 = final_zero
        for k in range(len(cat2_list)):
            data = transactions[(transactions['amount_type'] == type_list[i]) & 
                                (transactions['Category 1'] == cat1_list[j]) &
                                (transactions['Category 2'] == cat2_list[k])]
            piv = data.pivot_table(index=['amount_type','Category 1', 'Category 2', 'Category 3'],
                                   columns = 'month', values='amount', aggfunc='sum')
            piv['grand total'] = piv.sum(axis=1)
            if len(piv) > 0:
                final_cat2 = pd.concat([final_cat2, piv],sort = True)
            if (k == len(cat2_list)-1) and (len(final_cat2) > 0):
                final_cat2.loc[(type_list[i], cat1_list[j], 'total', '')] = final_cat2.sum()
        final_cat1 = pd.concat([final_cat1, final_cat2],sort = True)        
        if j == len(cat1_list)-1:
            final_cat1.loc[(type_list[i], 'total', '', '')] = final_cat1.sum() / 2
    final = pd.concat([final, final_cat1],sort = True)             
    if i == len(type_list)-1:
            final.loc[('total', '', '', '')] = final.sum() / 3           
                
# Выгрузка таблицы final в Google Sheets(cashflow_statement):
transactions_df_out = final.reset_index()
transactions_df_out = transactions_df_out.fillna(0)
a = transactions_df_out.columns.values.astype(str).tolist()
b = transactions_df_out.values.tolist()
b.insert(0, a)
results = service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, body = {
    "valueInputOption": "USER_ENTERED",
    "data": [
        {"range": "cashflow_statement!A4:z2000",
         "majorDimension": "ROWS",     
         "values": b}
            ]
}).execute()

# Выгрузка обработанной таблицы transactions в Google Sheets для формирования pivot_table:
transactions_df_out = transactions.reset_index()
transactions_df_out = transactions_df_out.fillna(0)
transactions_df_out[['date','month']] = transactions_df_out[['date','month']].astype('str')
transactions_df_out['amount'] = transactions_df_out['amount'].replace('.',',')
a = transactions_df_out.columns.values.astype(str).tolist()
b = transactions_df_out.values.tolist()
b.insert(0, a)
results = service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, body = {
    "valueInputOption": "USER_ENTERED",
    "data": [
        {"range": "transactions_for_pivot!A1:z2000",
         "majorDimension": "ROWS",     
         "values": b}
            ]
}).execute()