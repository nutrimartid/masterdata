# Import library

import time
import pandas as pd
import numpy as np
import datetime
import math
import os
import requests
import glob

sheet_url = "https://docs.google.com/spreadsheets/d/1lVkNt8fDNvxeToD_FJPnhR7H-TFUHb6ZKJPkPgbemPA/edit#gid=0"
url_reguler = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

# Import data
start_time = time.time()
print("Import Data ====== 1/10")

data_forstok = pd.read_excel(r'Input Data\forstok_new.xls', dayfirst = True)
data_forstok_pure = data_forstok.copy()

###tambahan 19 sept
convertcolumns=pd.read_excel("All Data\ord 1 to 2.xlsx")
data_forstok=data_forstok.rename(columns=convertcolumns[convertcolumns['Mapped'].notnull()][['V2','Mapped']].set_index('V2').to_dict()['Mapped'])
data_forstok['Order Voucher Amount']=None
data_forstok['Item Voucher Amount']=None
data_forstok['Item Voucher Platform']=None
data_forstok['Item Voucher Seller']=None
data_forstok=data_forstok[convertcolumns[convertcolumns['V1'].notnull()]['V1'].unique()]
# data_forstok=data_forstok[data_forstok['Store']!='Shopee']
data_forstok=data_forstok[data_forstok['Store']!='Blibli']
data_forstok=data_forstok[data_forstok['Store']!='Tiktok']
#data_forstok['Channel Order ID']=data_forstok['Channel Order ID'].str.replace('-','')
data_forstok['Channel Order ID']=data_forstok['Channel Order ID'].astype(str).str.replace('-','') #comment 28/12/22
print(data_forstok.head(10))
#data_forstok.loc[data_forstok['Warehouse Name']=='FBL Warehouse','Warehouse Name']='Primary Warehouse'
#data_forstok.loc[(data_forstok['Warehouse Name']=='Primary Warehouse')&~(data_forstok['Status'].isin(['Open','Cancelled'])),'Status']='Open'
###tambahan 19 sept

data_forstok = data_forstok.dropna(how = 'all')

list_skumiss = []

pd.options.mode.chained_assignment = None
# Forstok formatting
print("--- %s seconds ---" % (time.time() - start_time))
print("Formatting Data ====== 2/10")

if 'Unnamed: 35' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 35'], axis = 'columns')
if 'Unnamed: 36' in data_forstok:
    data_forstok = data_forstok.rename(columns={'Unnamed: 36' : 'Comment'})
if 'Unnamed: 37' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 37'], axis = 'columns')
if 'Unnamed: 38' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 38'], axis = 'columns')
if 'Unnamed: 39' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 39'], axis = 'columns')
if 'Unnamed: 40' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 40'], axis = 'columns')
if 'Unnamed: 41' in data_forstok:
    data_forstok = data_forstok.drop(['Unnamed: 41'], axis = 'columns')

data_forstok["Order Date"] = pd.to_datetime(data_forstok["Order Date"], errors = 'coerce')
data_forstok["Paid Date"] = pd.to_datetime(data_forstok["Paid Date"], errors = 'coerce')
data_forstok["Cancelled Date"] = pd.to_datetime(data_forstok["Cancelled Date"], errors = 'coerce')
data_forstok['Customer Name'] = data_forstok['Customer Name'].fillna(data_forstok['Shipping Name'])

# Phone formatting
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^620','0', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^62','0', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^620','0', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^8','08', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^21','021', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('^008','08', regex = True)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('(021)','021', regex = False)
data_forstok['Shipping Phone'] = data_forstok['Shipping Phone'].astype(str).str.replace('+62','0', regex = False)

# Master tatanama
data_SKU = pd.read_excel(r'SKU_File/data_SKU.xlsx')

s = requests.Session()
s.get("http://tatanama.pythonanywhere.com")
s.post("http://tatanama.pythonanywhere.com", data = {'username' : 'ecommerce', 'password' : 'ecommerce'})
r = s.get("http://tatanama.pythonanywhere.com/download")

with open('SKU_File/Master tatanama.xlsx', 'wb') as output:
    output.write(r.content)

if os.path.isfile('SKU_File/Master tatanama.xlsx') :
    SKU_append = pd.read_excel(r'SKU_File/Master tatanama.xlsx')
    SKU_append.columns = [x.replace('_', ' ') for x in SKU_append.columns]
    data_SKU = data_SKU[~data_SKU['SKU'].astype(str).isin(SKU_append['SKU'].astype(str))]
    data_SKU = data_SKU.append(SKU_append, ignore_index = True, sort = False)

to_excel = data_SKU.to_excel(r'SKU_File/data_SKU.xlsx', index = False)

# Forstok SKU
print("--- %s seconds ---" % (time.time() - start_time))
print("Fulfilling SKU ====== 3/10")

indeks = data_forstok[data_forstok['Item Name'].astype(str) == 'Buy 1 Get 1 FREE Tropicana Slim Goldenmil Vanilla Manuka Honey (6 Sch)'][data_forstok[data_forstok['Item Name'].astype(str) == 'Buy 1 Get 1 FREE Tropicana Slim Goldenmil Vanilla Manuka Honey (6 Sch)']['SKU'] == 'PE8B27'].index.to_list()
data_forstok['SKU'][indeks] = '2101384106P2'


skushopee = data_forstok[data_forstok['SKU'].astype(str).str.contains('(S)',regex = False)]
data_forstok = data_forstok[~data_forstok['SKU'].astype(str).isin(skushopee['SKU'].astype(str))]

skushopee = skushopee.reset_index(drop = True)
data_forstok = data_forstok.reset_index(drop = True)

forstok_all_sku = pd.read_excel(r'SKU_File\forstok_all_sku.xlsx')
indeks = data_forstok[data_forstok['SKU'].isnull()].index.to_list()

for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in data_SKU['Nama Produk'].astype(str).str.lower().str.strip().values:
        data_forstok['SKU'][i] = data_SKU['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == data_SKU['Nama Produk'].astype(str).str.lower().str.strip()].values[0]

indeks = data_forstok[data_forstok['SKU'].isnull()].index.to_list()
for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in forstok_all_sku['Item Name'].astype(str).str.lower().values:
        data_forstok['SKU'][i] = forstok_all_sku['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == forstok_all_sku['Item Name'].astype(str).str.lower()].values[0]

indeks = data_forstok[data_forstok['Item Name'].astype(str).str.contains(' - ')].index.to_list()
# Formatting double name
for i in indeks:
    if data_forstok['Item Name'][i].count(' - ') == 1 :
        if (data_forstok['Item Name'][i].split(' - ')[0] == data_forstok['Item Name'][i].split(' - ')[1]):
            data_forstok['Item Name'][i] = data_forstok['Item Name'][i].split(' - ')[0]
    elif data_forstok['Item Name'][i].count(' - ') > 1:
        temp = math.ceil(data_forstok['Item Name'][i].count(' - ')/2)
        itemname = ''
        duplicate = ''
        for j in range(temp):
            if j == 0:
                itemname = itemname + data_forstok['Item Name'][i].split(' - ')[j]
                duplicate = duplicate + data_forstok['Item Name'][i].split(' - ')[temp+j]
            else :
                itemname = itemname + ' ' +data_forstok['Item Name'][i].split(' - ')[j]
                duplicate = duplicate + ' ' +data_forstok['Item Name'][i].split(' - ')[temp+j]
        if itemname == duplicate:
            data_forstok['Item Name'][i] = itemname

data_forstok['Item Name'] = data_forstok['Item Name'].str.replace(r' - $', '')
data_forstok["Item Name"] = data_forstok["Item Name"].str.replace('Special Promo by Nutrimart Serba 12 RIBU Varian:', '', regex=False)
data_forstok["Item Name"] = data_forstok["Item Name"].str.replace('Khusus Jabodetabek', '- Khusus Jabodetabek', regex=False)
data_forstok['Item Name'] = data_forstok['Item Name'].str.replace('Buy 1 Get F', 'Buy 1 Get 1 F')
data_forstok['Item Name'] = data_forstok['Item Name'].str.replace('Buy 1 Get H', 'Buy 1 Get 1 H')
data_forstok['Item Name'] = data_forstok['Item Name'].str.replace('Buy 12 FREE', 'Buy 12 FREE 12')

# Formatting SKU based on name
indeks = data_forstok[data_forstok['SKU'].isnull()].index.to_list()
for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in data_SKU['Nama Produk'].astype(str).str.lower().values:
        data_forstok['SKU'][i] = data_SKU['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == data_SKU['Nama Produk'].astype(str).str.lower()].values[0]

indeks = data_forstok[data_forstok['SKU'].isnull()].index.to_list()
for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in forstok_all_sku['Item Name'].astype(str).str.lower().values:
        data_forstok['SKU'][i] = forstok_all_sku['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == forstok_all_sku['Item Name'].astype(str).str.lower()].values[0]

indeks = data_forstok[data_forstok['SKU'].isnull()].index.tolist()

data_adasku = data_forstok[['Item Name', 'SKU']]
data_adasku = data_adasku[data_adasku['SKU'].notnull()]

data_nosku = data_forstok[['Item Name', 'SKU']]
data_nosku = data_nosku[data_nosku['SKU'].isnull()]

for i in indeks:
    if data_adasku['SKU'].loc[data_nosku['Item Name'][i] == data_adasku['Item Name']].size != 0:
        data_forstok['SKU'][i] = data_adasku['SKU'].loc[data_nosku['Item Name'][i] == data_adasku['Item Name']].values[0]

for i in indeks:
    if data_forstok['Item Name'][i] == 'Lokalate Kopi Durian 10s':
        data_forstok['SKU'][i] = '1101675318'
    elif data_forstok['Item Name'][i] == 'Nutrisari Madu Kurma Isi 16 Renceng X 10 Sachet Karton' or data_forstok['Item Name'][i] =='Nutrisari Madu Kurma Isi 16 Renceng X 10 SachetKarton':
        data_forstok['SKU'][i] = 'PN30(16)'
    elif data_forstok['Item Name'][i] == 'L-Men Protein Bar Crunchy Chocolate Isi X12 (Exp Date:10-Apr-2019)':
        data_forstok['SKU'][i] = '2306592173'
    elif data_forstok['Item Name'][i] == 'FS Hilo Active Chocolate Minuman Kesehatan [750 gr]' or data_forstok['Item Name'][i] == 'FSHilo Active Chocolate Minuman Kesehatan [750 gr]':
        data_forstok['SKU'][i] = '2101452190'
    elif data_forstok['Item Name'][i] == 'FS L-Men Platinum Suplemen Kesehatan + Free Spider Bottle [800 g] Hitam' or data_forstok['Item Name'][i] == 'FSL-Men Platinum Suplemen Kesehatan + Free Spider Bottle [800 g] Hitam':
        data_forstok['SKU'][i] = '2305551288P1G26'
    elif data_forstok['Item Name'][i] == 'NutriSari Premium ala Jus Mangga':
        data_forstok['SKU'][i] = '1100534104'
    elif data_forstok['Item Name'][i] == 'Buy 1 Get 1 FREE Tropicana Slim Sweetener Honey (50 Sch) - FS':
        data_forstok['SKU'][i] = '2102501125P1G53'

indeks = data_forstok[data_forstok['Item Name'] == 'Buy 1 Get 1 FREE Tropicana Slim Goldenmil Vanilla Manuka Honey (6 Sch)'].index.to_list()
data_forstok['SKU'][indeks] = 'PB37T43'

indeks = data_forstok[~data_forstok['SKU'].astype(str).isin(data_SKU['SKU'].astype(str))].index.to_list()
for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in forstok_all_sku['Item Name'].astype(str).str.lower().values:
        data_forstok['SKU'][i] = forstok_all_sku['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == forstok_all_sku['Item Name'].astype(str).str.lower()].values[0]

list_alias = []
list_alias_name = []
for colname in data_SKU.columns:
    if 'Alias SKU' in colname:
        list_alias.append(colname)
    if 'Alias Nama' in colname:
        list_alias_name.append(colname)


for i in indeks:
    for j in list_alias:
        if str(data_forstok['SKU'][i]) in data_SKU[j].astype(str).values:
            idx = data_SKU[str(data_forstok['SKU'][i]) == data_SKU[j].astype(str)].index.to_list()
            for k in idx:
                if str(data_forstok['Item Name'][i]) == data_SKU[j.replace('SKU', 'Nama')][k]:
                    data_forstok['SKU'][i] = data_SKU['SKU'][k]

indeks = data_forstok[~data_forstok['SKU'].astype(str).isin(data_SKU['SKU'].astype(str))].index.to_list()

for i in indeks:
    for j in list_alias_name:
        if str(data_forstok['Item Name'][i]).lower() in data_SKU[j].astype(str).str.lower().values:
            data_forstok['SKU'][i] = data_SKU['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == data_SKU[j].astype(str).str.lower()].values[0]

indeks = data_forstok[~data_forstok['SKU'].astype(str).isin(data_SKU['SKU'].astype(str))].index.to_list()

for i in indeks:
    if str(data_forstok['Item Name'][i]).lower() in data_SKU['Nama Produk'].astype(str).str.lower().values:
        data_forstok['SKU'][i] = data_SKU['SKU'].loc[str(data_forstok['Item Name'][i]).lower() == data_SKU['Nama Produk'].astype(str).str.lower()].values[0]

indeks = skushopee[~skushopee['SKU'].astype(str).str.replace('(S)','', regex = False).isin(data_SKU['SKU'].astype(str))].index.to_list()
for i in indeks:
    if str(skushopee['Item Name'][i]).lower() in data_SKU['Nama Produk'].astype(str).str.lower().values:
        skushopee['SKU'][i] = data_SKU['SKU'].loc[str(skushopee['Item Name'][i]).lower() == data_SKU['Nama Produk'].astype(str).str.lower()].values[0]
        skushopee['SKU'][i] = '(S)' + str(skushopee['SKU'][i])

indeks = skushopee[~skushopee['SKU'].astype(str).str.replace('(S)','', regex = False).isin(data_SKU['SKU'].astype(str))].index.to_list()
for i in indeks:
    for j in list_alias:
        if str(skushopee['SKU'][i]).replace('(S)','') in data_SKU[j].astype(str).values:
            idx = data_SKU[str(skushopee['SKU'][i]).replace('(S)','') == data_SKU[j].astype(str)].index.to_list()
            for k in idx:
                if str(skushopee['Item Name'][i]) == data_SKU[j.replace('SKU', 'Nama')][k]:
                    skushopee['SKU'][i] = data_SKU['SKU'][k]

indeks = data_forstok[data_forstok['SKU'].astype(str) == 'Gift Sosro'].index.to_list()
data_forstok = data_forstok.drop(indeks, axis = 0)
data_forstok = data_forstok.reset_index(drop = True)

# indeks = data_forstok[data_forstok['SKU'].astype(str) == '2101453190'].index.to_list()
# data_forstok['SKU'][indeks] = 'PH9G122'

# indeks = data_forstok[data_forstok['SKU'].astype(str) == '2101453180'].index.to_list()
# data_forstok['SKU'][indeks] = 'PH8G122'

# indeks = data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])][data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])]['SKU'].astype(str) == '2101428180'].index.to_list()
# data_forstok['SKU'][indeks] = 'PH4G122'

# indeks = data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])][data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])]['SKU'].astype(str) == '2101428190'].index.to_list()
# data_forstok['SKU'][indeks] = 'PH5G122'

# indeks = data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])][data_forstok[data_forstok['Channel'].isin(['Blibli', 'Bukalapak', 'Tokopedia'])]['SKU'].astype(str) == '2101401180'].index.to_list()
# data_forstok['SKU'][indeks] = 'PH3G122'

indeks = data_forstok[data_forstok['SKU'].astype(str) == 'PN65(2)B44(2)'].index.to_list()
data_forstok['SKU'][indeks] = '1101588453'

indeks = data_forstok[data_forstok['SKU'].astype(str) == 'PB48(2)N68(2)'].index.to_list()
data_forstok['SKU'][indeks] = '1101989453'

indeks = data_forstok[data_forstok['SKU'].astype(str) == 'PN68(2)B48(2)'].index.to_list()
data_forstok['SKU'][indeks] = '1101989453'

indeks = data_forstok[data_forstok['SKU'].astype(str) == 'PN76(2)B54(2)'].index.to_list()
data_forstok['SKU'][indeks] = '1101930453'

print("--- %s seconds ---" % (time.time() - start_time))
print("Listing SKU Missing ====== 4/10")
idx = []
idx = idx + data_forstok[data_forstok['SKU'].isnull()].drop_duplicates().index.to_list()
idx = idx + data_forstok[~data_forstok['SKU'].astype(str).isin(data_SKU['SKU'].astype(str))].index.to_list()
idx = list(dict.fromkeys(idx))
idx_s = skushopee[~skushopee['SKU'].astype(str).str.replace('(S)','', regex = False).isin(data_SKU['SKU'].astype(str))].index.to_list()
idx_s = list(dict.fromkeys(idx_s))

to_excel = data_forstok.to_excel(r'Input Data\forstok_new_after_run.xls', index = False)


print("--- %s seconds ---" % (time.time() - start_time))
if len(idx) != 0 or len(idx_s) != 0:
# if len(idx) != 0:
    alert = data_forstok.iloc[idx, ][['SKU', 'Item Name', 'Channel']].drop_duplicates()
    alert = alert.append(skushopee.iloc[idx_s][['SKU', 'Item Name', 'Channel']].drop_duplicates(), ignore_index = True, sort = False)
    alert['SKU Valid'] = np.nan
    to_excel = alert.to_excel('ALERT_FORSTOK_SKU_MISSING.xlsx')
    print("Some SKU Missing Please Complete It ====== 5/10")
    print("--- %s seconds ---" % (time.time() - start_time))

    print("Appending to old data ===== Just leave the program running")
    forstok_old = pd.read_excel(r'All Data\data_forstok_2019.xlsx')
    forstok_old = forstok_old.append(data_forstok_pure, ignore_index = True, sort = False)
    forstok_old = forstok_old.drop_duplicates(['Channel Order ID', 'Order Date', 'Item Name', 'Sales Order ID', 'Quantity', 'Sub Total', 'Seller Discount'], keep = 'last')
    to_excel = forstok_old.to_excel(r'All Data\data_forstok_2019.xlsx', index = False)
    print("--- %s seconds ---" % (time.time() - start_time))
else :
    data_forstok = data_forstok.append(skushopee, ignore_index = True, sort = False)
    data_forstok = data_forstok.reset_index(drop = True)
    print("Preparing for WMS")
    print("Unbundling WMS ===== 1/5")
    forstok_WMS = data_forstok.copy()
    forstok_WMS = forstok_WMS[forstok_WMS['Status'].isin(['Open', 'Open, Shipped', 'Shipped, Open', 'Delivered, Shipped, Open', 'Ready to Ship, Open', 'Printed', 'Ready to Ship'])]
# forstok_WMS['SKU'] = forstok_WMS['SKU'].astype(str).str.replace('(S)','',regex = False)
    indeks = forstok_WMS[forstok_WMS['SKU'].astype(str) == 'PN61(2)B41(2)'].index.to_list()
    forstok_WMS['SKU'][indeks] = '1101987453'

    shopee_WMS = forstok_WMS[forstok_WMS['SKU'].astype(str).str.contains('(S)',regex = False)]
    forstok_WMS = forstok_WMS[~forstok_WMS['SKU'].astype(str).isin(shopee_WMS['SKU'].astype(str))]
    list_drop = []
    forstok_WMS = forstok_WMS.reset_index(drop = True)
    WMS_Eval = forstok_WMS.copy()

    # check_500 = forstok_WMS.groupby('Sales Order ID')['Sub Total'].sum().reset_index()
    # check_500 = check_500[check_500['Sub Total'] >= 500000]['Sales Order ID'].unique()

    # indeks = forstok_WMS[forstok_WMS['Sales Order ID'].isin(check_500)].index.to_list()
    # orders = []
    # for i in indeks:
    #     if forstok_WMS['Sales Order ID'][i] not in orders:
    #         idx = data_SKU[data_SKU['SKU'] == '(G)71210198'].index[0]
    #         new_data = forstok_WMS.iloc[i,]
    #         new_data['Item Name'] = data_SKU['Nama Produk'][idx]
    #         new_data['Quantity'] = 1
    #         new_data['Selling Price'] = 0
    #         new_data['SKU'] = '(G)71210198'
    #         new_data['Quantity'] = str(new_data['Quantity']).replace('.0','')
    #         new_data['Selling Price'] = str(new_data['Selling Price']).replace('.0','')
    #         forstok_WMS = forstok_WMS.append(new_data, ignore_index = True)
    #         WMS_Eval = WMS_Eval.append(new_data, ignore_index = True)
    #         orders.append(forstok_WMS['Sales Order ID'][i])

    forstok_WMS = forstok_WMS.reset_index(drop = True)

    # list_gift = ['PL26B103(3)', 'PL26B102(3)', 'PL21L26']
    # gift_order = forstok_WMS[(forstok_WMS['SKU'].astype(str).isin(list_gift)) & (forstok_WMS['Channel'] == 'Shopee')]['Sales Order ID'].astype(str).to_list()


    indeks = forstok_WMS[forstok_WMS['SKU'].isin(data_SKU[data_SKU['Brand'] == 'Bundle']['SKU'])].index.to_list()
    for i in indeks:
        if str(forstok_WMS['SKU'][i]) == "PN20N35(2)T3T22T43G105":
            pass
        else :
            if str(forstok_WMS['SKU'][i]) in data_SKU['SKU'].astype(str).values:
                idx = data_SKU[str(forstok_WMS['SKU'][i] ) == data_SKU['SKU'].astype(str)].index[0]
                for j in range(1,8):
                    colname = 'Produk ' + str(j)
                    if str(data_SKU[colname][idx]) != 'nan':
                        new_data = forstok_WMS.iloc[i,]
                        new_data['Item Name'] = data_SKU[colname][idx]
                        new_data['Selling Price'] = data_SKU['Subtotal ' + colname][idx] * new_data['Quantity']
                        new_data['Quantity'] = new_data['Quantity'] * data_SKU['PCS ' + colname][idx]
                        new_data['SKU'] = str(data_SKU['SKU ' + colname][idx]).replace('.0','')
                        new_data['Quantity'] = str(new_data['Quantity']).replace('.0','')
                        new_data['Selling Price'] = str(new_data['Selling Price']).replace('.0','')
                        forstok_WMS = forstok_WMS.append(new_data, ignore_index = True)
                        WMS_Eval = WMS_Eval.append(new_data, ignore_index = True)
                        list_drop.append(i)
    forstok_WMS = forstok_WMS.drop(list_drop, axis = 0)
    forstok_WMS = forstok_WMS.reset_index(drop = True)

    # indeks = forstok_WMS[forstok_WMS['SKU'].isin(data_SKU[data_SKU['Brand'] == 'L-Men']['SKU'])].index.to_list()
    # orders = []
    # for i in indeks:
    #     if forstok_WMS['Sales Order ID'][i] not in orders:
    #         idx = data_SKU[data_SKU['SKU'] == '(B)71210138'].index[0]
    #         new_data = forstok_WMS.iloc[i,]
    #         new_data['Item Name'] = data_SKU['Nama Produk'][idx]
    #         new_data['Quantity'] = 1
    #         new_data['Selling Price'] = 0
    #         new_data['SKU'] = '(B)71210138'
    #         new_data['Quantity'] = str(new_data['Quantity']).replace('.0','')
    #         new_data['Selling Price'] = str(new_data['Selling Price']).replace('.0','')
    #         forstok_WMS = forstok_WMS.append(new_data, ignore_index = True)
    #         WMS_Eval = WMS_Eval.append(new_data, ignore_index = True)
    #         orders.append(forstok_WMS['Sales Order ID'][i])
    # forstok_WMS = forstok_WMS.reset_index(drop = True)

    list_drop = []
    shopee_WMS = shopee_WMS.reset_index(drop= True)
    indeks = shopee_WMS[shopee_WMS['SKU'].astype(str).str.replace('(S)','', regex = False).isin(data_SKU[data_SKU['Brand'] == 'Bundle']['SKU'])].index.to_list()
    for i in indeks:
        if str(shopee_WMS['SKU'][i]).replace('(S)','') in data_SKU['SKU'].astype(str).values:
            idx = data_SKU[str(shopee_WMS['SKU'][i]).replace('(S)','') == data_SKU['SKU'].astype(str)].index[0]
            for j in range(1,8):
                colname = 'Produk ' + str(j)
                if str(data_SKU[colname][idx]) != 'nan':
                    new_data = shopee_WMS.iloc[i,]
                    new_data['Item Name'] = data_SKU[colname][idx]
                    new_data['SKU'] = '(S)' + str(data_SKU['SKU ' + colname][idx])
                    new_data['Selling Price'] = data_SKU['Subtotal ' + colname][idx] * new_data['Quantity']
                    new_data['Quantity'] = new_data['Quantity'] * data_SKU['PCS ' + colname][idx]
                    new_data['SKU'] = str(new_data['SKU']).replace('.0','')
                    new_data['Quantity'] = str(new_data['Quantity']).replace('.0','')
                    new_data['Selling Price'] = str(new_data['Selling Price']).replace('.0','')
                    shopee_WMS = shopee_WMS.append(new_data, ignore_index = True)
                    WMS_Eval = WMS_Eval.append(new_data, ignore_index = True)
                    list_drop.append(i)
    shopee_WMS = shopee_WMS.drop(list_drop, axis = 0)
    shopee_WMS = shopee_WMS.reset_index(drop = True)

    indeks = shopee_WMS[shopee_WMS['SKU'].astype(str).str.replace('(S)','', regex = False).isin(data_SKU[data_SKU['Brand'] == 'L-Men']['SKU'])].index.to_list()
    for i in indeks:
        if shopee_WMS['Sales Order ID'][i] not in orders:
            idx = data_SKU[data_SKU['SKU'] == '(B)71210138'].index[0]
            new_data = shopee_WMS.iloc[i,]
            new_data['Item Name'] = data_SKU['Nama Produk'][idx]
            new_data['Quantity'] = 1
            new_data['Selling Price'] = 0
            new_data['SKU'] = '(B)71210138'
            new_data['Quantity'] = str(new_data['Quantity']).replace('.0','')
            new_data['Selling Price'] = str(new_data['Selling Price']).replace('.0','')
            shopee_WMS = shopee_WMS.append(new_data, ignore_index = True)
            WMS_Eval = WMS_Eval.append(new_data, ignore_index = True)
            orders.append(shopee_WMS['Sales Order ID'][i])
    shopee_WMS = shopee_WMS.reset_index(drop = True)

    forstok_WMS = forstok_WMS.append(shopee_WMS, ignore_index = True, sort = False)

    print("--- %s seconds ---" % (time.time() - start_time))
    print("Fill Invoice Number ===== 2/5")
    forstok_WMS['Invoice Number'] = np.nan
    for i in range(forstok_WMS.shape[0]):
        if forstok_WMS['Channel'][i] == 'Tokopedia' or forstok_WMS['Channel'][i] == 'Bukalapak' or forstok_WMS['Channel'][i] == 'Elevenia':
            temp = str(forstok_WMS['Sales Order ID'][i])
            forstok_WMS['Sales Order ID'][i] = str(forstok_WMS['Channel Order ID'][i])
            forstok_WMS['Invoice Number'][i] = str(forstok_WMS['Channel Order ID'][i])
            forstok_WMS['Channel Order ID'][i] = temp.replace('#SO-', 'SO')
        elif forstok_WMS['Channel'][i] == 'Lazada' or forstok_WMS['Channel'][i] == 'Blibli'or forstok_WMS['Channel'][i] == 'JD Indonesia':
            forstok_WMS['Invoice Number'][i] = forstok_WMS['Channel Order ID'][i]
    print("--- %s seconds ---" % (time.time() - start_time))
    print("Formatting Data ===== 3/5")

    WMS_Eval = WMS_Eval.reset_index(drop = True)
    WMS_Eval['Invoice Number'] = np.nan
    for i in range(WMS_Eval.shape[0]):
        if WMS_Eval['Channel'][i] == 'Tokopedia' or WMS_Eval['Channel'][i] == 'Bukalapak' or WMS_Eval['Channel'][i] == 'Elevenia':
            temp = str(WMS_Eval['Sales Order ID'][i])
            WMS_Eval['Sales Order ID'][i] = str(WMS_Eval['Channel Order ID'][i])
            WMS_Eval['Invoice Number'][i] = str(WMS_Eval['Channel Order ID'][i])
            WMS_Eval['Channel Order ID'][i] = temp.replace('#SO-', 'SO')
        elif WMS_Eval['Channel'][i] == 'Lazada' or WMS_Eval['Channel'][i] == 'Blibli' or WMS_Eval['Channel'][i] == 'JD Indonesia' or WMS_Eval['Channel'][i] == 'Shopee' or WMS_Eval['Channel'][i] == 'Aladin Mall':
            WMS_Eval['Invoice Number'][i] = WMS_Eval['Channel Order ID'][i]

    forstok_WMS = forstok_WMS.dropna(subset =['Order Date'])
    # forstok_WMS = forstok_WMS[forstok_WMS['Channel']!='Shopee']
    forstok_WMS = forstok_WMS[forstok_WMS['Channel']!='FBL']
    forstok_tokped = forstok_WMS[forstok_WMS['Warehouse Name']=='Tokopedia Warehouse']
    print('cek pertama')
    print(forstok_WMS['Channel'].unique())
    forstok_WMS.loc[forstok_WMS['Channel']=='TikTok','Warehouse Name']='Primary Warehouse'
    forstok_WMS = forstok_WMS[forstok_WMS['Warehouse Name']=='Primary Warehouse']


    forstok_WMS['Shipping Address2'] = forstok_WMS['Shipping Address2'].fillna(0)
    forstok_WMS['AWB'] = forstok_WMS['AWB'].fillna(0)

    list_NS10 = ['1101531451','1101572016','1101907019','1101909451','1101976451','1101558017','1101569451','1101907451','1102070451','1101573451','1101572451','1101978451','1101979451','1101909019']
    noorder = forstok_WMS[forstok_WMS['SKU'].astype(str).isin(list_NS10)]['Sales Order ID'].astype(str).to_list()
    WMS_Eval = WMS_Eval[~WMS_Eval['Sales Order ID'].astype(str).isin(noorder)]

    wms_blibli = forstok_WMS[forstok_WMS['Channel'] == 'Blibli'][forstok_WMS[forstok_WMS['Channel'] == 'Blibli']['Status'] == 'Open'].copy()
    wms_bl = forstok_WMS[forstok_WMS['Channel'] == 'Bukalapak'][forstok_WMS[forstok_WMS['Channel'] == 'Bukalapak']['Status'] == 'Open'].copy()
    wms_jd = forstok_WMS[forstok_WMS['Channel'] == 'JD Indonesia'][forstok_WMS[forstok_WMS['Channel'] == 'JD Indonesia']['Status'] == 'Open'].copy()
    wms_lazada = forstok_WMS[forstok_WMS['Channel'] == 'Lazada'][forstok_WMS[forstok_WMS['Channel'] == 'Lazada']['Status'].isin(['Open', 'Open, Shipped', 'Shipped, Open', 'Delivered, Shipped, Open', 'Ready to Ship, Open', 'Delivered, Open', 'Open, Delivered'])].copy()
    wms_lazada['Shipping Courier'] = wms_lazada['Shipping Courier'].fillna('LEX')
    wms_tokped = forstok_WMS[forstok_WMS['Channel'] == 'Tokopedia'][forstok_WMS[forstok_WMS['Channel'] == 'Tokopedia']['Status'] == 'Open'].copy()
    wms_elevenia = forstok_WMS[forstok_WMS['Channel'] == 'Elevenia'][forstok_WMS[forstok_WMS['Channel'] == 'Elevenia']['Status'] == 'Open'].copy()
    wms_shopee = forstok_WMS[forstok_WMS['Channel'] == 'Shopee'][forstok_WMS[forstok_WMS['Channel'] == 'Shopee']['Status'] == 'Open'].copy()
    wms_aladin = forstok_WMS[forstok_WMS['Channel'] == 'Aladin Mall'][forstok_WMS[forstok_WMS['Channel'] == 'Aladin Mall']['Status'] == 'Open'].copy()
    wms_tiktok = forstok_WMS[forstok_WMS['Channel'] == 'TikTok'][forstok_WMS[forstok_WMS['Channel'] == 'TikTok']['Status'] == 'Open'].copy()

    print(forstok_WMS['Channel'].unique())
    data_WMS = wms_blibli.append([wms_bl, wms_jd, wms_lazada, wms_tokped, wms_elevenia, wms_shopee, wms_aladin,wms_tiktok], ignore_index=True, sort=False)
    printed_WMS = forstok_WMS[~forstok_WMS['Sales Order ID'].astype(str).isin(data_WMS['Sales Order ID'].astype(str))][forstok_WMS[~forstok_WMS['Sales Order ID'].astype(str).isin(data_WMS['Sales Order ID'].astype(str))]['Status'].isin(['Printed', 'Ready to Ship'])]

    data_WMS = data_WMS.reset_index(drop = True)
    printed_WMS = printed_WMS.reset_index(drop = True)

    data_WMS = data_WMS[['Order Date', 'Channel', 'Sales Order ID', 'Channel Order ID', 'Invoice Number', 'Customer Name',
                        'Item Name', 'SKU', 'Quantity', 'Selling Price', 'Shipping', 'Shipping Name', 'Shipping Address1',
                        'Shipping Address2', 'Shipping City', 'Shipping Zip', 'Shipping Province', 'Shipping Country',
                        'Shipping Phone', 'Shipping Courier', 'AWB', 'Notes']]

    printed_WMS = printed_WMS[['Order Date', 'Channel', 'Sales Order ID', 'Channel Order ID', 'Invoice Number', 'Customer Name',
                            'Item Name', 'SKU', 'Quantity', 'Selling Price', 'Shipping', 'Shipping Name', 'Shipping Address1',
                            'Shipping Address2', 'Shipping City', 'Shipping Zip', 'Shipping Province', 'Shipping Country',
                            'Shipping Phone', 'Shipping Courier', 'AWB', 'Notes']]

    data_WMS = data_WMS.rename(columns={'Order Date' : 'Order date','Selling Price' : 'Price', 'Shipping' : 'Shipping Cost'})
    printed_WMS = printed_WMS.rename(columns={'Order Date' : 'Order date','Selling Price' : 'Price', 'Shipping' : 'Shipping Cost'})
    for i in range(data_WMS.shape[0]):
        if int(data_WMS['Order date'][i].strftime('%d')) <= 12:
            data_WMS['Order date'][i] = pd.to_datetime(data_WMS['Order date'][i].strftime('%Y-%d-%m %H:%M'))
        else :
            data_WMS['Order date'][i] = pd.to_datetime(data_WMS['Order date'][i])
    print("--- %s seconds ---" % (time.time() - start_time))
    print("Filter NS 10s and Tokped Cabang ===== 4/5")

    # list_NS10 = ['1101531451','1101572016','1101907019','1101909451','1101976451','1101558017','1101569451','1101907451','1102070451','1101573451','1101572451','1101978451','1101979451','1101909019']
    # noorder = data_WMS[data_WMS['SKU'].astype(str).isin(list_NS10)]['Sales Order ID'].astype(str).to_list()
    # WMS_Not = data_WMS[data_WMS['Sales Order ID'].astype(str).isin(noorder)]

    # noorder = data_WMS[data_WMS['Sales Order ID'].astype(str).isin(gift_order)]
    # noorder['Tanggal'] = pd.to_datetime(noorder['Order date']).dt.day
    # noorder['Bulan'] = pd.to_datetime(noorder['Order date']).dt.month_name()

    # regular = pd.read_csv(url_reguler)
    # regular['Nomor Handphone'] = regular['Nomor Handphone'].astype(str).str.replace('^62', '0', regex = True)
    # regular['Tanggal'] = pd.to_datetime(regular['Tanggal']).dt.day
    # regular['Bulan'] = pd.to_datetime(regular['Tanggal']).dt.month_name()

    # noorder = noorder.merge(regular[['No','Tanggal', 'Bulan', 'Nomor Handphone']].drop_duplicates(['Tanggal', 'Bulan', 'Nomor Handphone']), how = 'left',
    #                         left_on = ['Tanggal', 'Bulan', 'Shipping Phone'], right_on = ['Tanggal', 'Bulan', 'Nomor Handphone'])
    # print(noorder[noorder['No'].notnull()].head())
    # noorder = noorder[noorder['No'].isnull()]['Sales Order ID'].astype(str).to_list()

    # WMS_Not = data_WMS[data_WMS['Sales Order ID'].astype(str).isin(noorder)]

    # data_WMS = data_WMS[~data_WMS['Sales Order ID'].astype(str).isin(noorder)]
    # printed_WMS = printed_WMS[~printed_WMS['Sales Order ID'].astype(str).isin(noorder)]

    # tokped_cabang = pd.read_excel(r'Input Data\tokopedia_new.xlsx', header = 3)
    # tokped_cabang = tokped_cabang[tokped_cabang['Jenis Layanan'] == 'Dilayani Toko Cabang']

    # tokped_cabang_wms = data_WMS[data_WMS['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    # temp = printed_WMS[printed_WMS['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    # tokped_cabang_wms = tokped_cabang_wms.append(temp, ignore_index = True, sort = False)
    # temp = WMS_Not[WMS_Not['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    # tokped_cabang_wms = tokped_cabang_wms.append(temp, ignore_index = True, sort = False)
    # forstok_tokped = forstok_tokped[forstok_tokped['Status'].isin(['Printed', 'Ready to Ship', 'Open'])]
    # tokped_cabang_wms = tokped_cabang_wms.append(forstok_tokped, ignore_index = True, sort = False)

    # data_WMS = data_WMS[~data_WMS['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    # printed_WMS = printed_WMS[~printed_WMS['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    # WMS_Not = WMS_Not[~WMS_Not['Sales Order ID'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]

    print("Exporting WMS ===== 5/5")
    data_WMS = data_WMS.drop('Notes', axis = 1)
    data_WMS['Channel Order ID']=data_WMS['Channel Order ID'].str.replace('-','')  ##timo
    printed_WMS = printed_WMS.drop('Notes', axis = 1)
    to_excel = data_WMS.to_excel('WMS\data_WMS.xlsx', index = False)
    to_excel = printed_WMS.to_excel('WMS\All_Status_WMS.xlsx', index = False)
    # to_excel = WMS_Not.to_excel('WMS\WMS_Gift_Shopee.xlsx', index = False)

    print("Export WMS Finish")
    print("--- %s seconds ---" % (time.time() - start_time))
    print("WMS Ready to Use, dont close the program")
    print("Saving WMS Data to WMS Historical")
    list_of_files = glob.glob('Clean Data/WMS Historical/*')
    latest_file = max(list_of_files, key=os.path.getctime)

    WMS_historis = pd.read_excel(str(latest_file))
    WMS_historis = WMS_historis[~WMS_historis['Sales Order ID'].astype(str).isin(data_WMS['Sales Order ID'].astype(str))]
    WMS_historis = WMS_historis.append(data_WMS, ignore_index = True, sort = False)

    WMS_historis = WMS_historis[~WMS_historis['Sales Order ID'].astype(str).isin(printed_WMS['Sales Order ID'].astype(str))]
    WMS_historis = WMS_historis.append(printed_WMS, ignore_index = True, sort = False)

    WMS_historis = WMS_historis[~WMS_historis['Sales Order ID'].astype(str).isin(WMS_Not['Sales Order ID'].astype(str))]
    WMS_historis = WMS_historis.append(WMS_Not, ignore_index = True, sort = False)

    from datetime import datetime
    WMS_historis.to_excel(r'Clean Data/WMS Historical/WMS Historical ' + str(datetime.today().date())  + '.xlsx', index = False)

    data_tokped_cabang = pd.read_excel(r'WMS/Tokped Toko Cabang/Tokped Toko Cabang.xlsx')
    data_tokped_cabang = data_tokped_cabang[~data_tokped_cabang['Invoice'].astype(str).isin(tokped_cabang['Invoice'].astype(str))]
    data_tokped_cabang = data_tokped_cabang.append(tokped_cabang, ignore_index = True, sort = False)
    data_tokped_cabang.to_excel(r'WMS/Tokped Toko Cabang/Tokped Toko Cabang.xlsx', index = False)

    data_tokped_cabang_wms = pd.read_excel(r'WMS/Tokped Toko Cabang/Tokped Toko Cabang WMS Form.xlsx')
    data_tokped_cabang_wms = data_tokped_cabang_wms[~data_tokped_cabang_wms['Sales Order ID'].astype(str).isin(tokped_cabang_wms['Sales Order ID'].astype(str))]
    data_tokped_cabang_wms = data_tokped_cabang_wms.append(tokped_cabang_wms, ignore_index = True, sort = False)
    data_tokped_cabang_wms.to_excel(r'WMS/Tokped Toko Cabang/Tokped Toko Cabang WMS Form.xlsx', index = False)

    WMS_Eval = WMS_Eval.dropna(subset =['Order Date'])
    WMS_Eval = WMS_Eval[WMS_Eval['Channel']!='Shopee']
    WMS_Eval = WMS_Eval[WMS_Eval['Channel']!='FBL']

    wms_blibli = WMS_Eval[WMS_Eval['Channel'] == 'Blibli'][WMS_Eval[WMS_Eval['Channel'] == 'Blibli']['Status'] == 'Open'].copy()
    wms_bl = WMS_Eval[WMS_Eval['Channel'] == 'Bukalapak'][WMS_Eval[WMS_Eval['Channel'] == 'Bukalapak']['Status'] == 'Open'].copy()
    wms_jd = WMS_Eval[WMS_Eval['Channel'] == 'JD Indonesia'][WMS_Eval[WMS_Eval['Channel'] == 'JD Indonesia']['Status'] == 'Open'].copy()
    wms_lazada = WMS_Eval[WMS_Eval['Channel'] == 'Lazada'][WMS_Eval[WMS_Eval['Channel'] == 'Lazada']['Status'].isin(['Open', 'Open, Shipped', 'Shipped, Open', 'Delivered, Shipped, Open', 'Ready to Ship, Open'])].copy()
    wms_lazada['Shipping Courier'] = wms_lazada['Shipping Courier'].fillna('LEX')
    wms_tokped = WMS_Eval[WMS_Eval['Channel'] == 'Tokopedia'][WMS_Eval[WMS_Eval['Channel'] == 'Tokopedia']['Status'] == 'Open'].copy()

    data_WMS = wms_blibli.append([wms_bl, wms_jd, wms_lazada, wms_tokped], ignore_index = True, sort = False)

    data_WMS = data_WMS[['Order Date', 'Channel', 'Sales Order ID', 'Channel Order ID', 'Invoice Number', 'Customer Name',
                            'Item Name', 'SKU', 'Quantity', 'Selling Price', 'Shipping', 'Shipping Name', 'Shipping Address1',
                            'Shipping Address2', 'Shipping City', 'Shipping Zip', 'Shipping Province', 'Shipping Country',
                            'Shipping Phone', 'Shipping Courier', 'AWB']]

    data_WMS = data_WMS.rename(columns={'Order Date' : 'Order date','Selling Price' : 'Price', 'Shipping' : 'Shipping Cost'})
    to_excel = data_WMS.to_excel('WMS\Bundled_WMS.xlsx', index = False)


    print("Preparing Appending Forstok to Masterdata")

    # Brand Data
    print("Filling Brand ====== 5/10")
    data_forstok['SKU'] = data_forstok['SKU'].astype(str)
    data_forstok['Item Name'] = data_forstok['Item Name'].astype(str)
    data_SKU['Real SKU'] = data_SKU['SKU'].astype(str)
    data_SKU['Real Nama Produk'] = data_SKU['Nama Produk'].astype(str)

    data_forstok = data_forstok.merge(data_SKU[['Real SKU', 'Real Nama Produk']].drop_duplicates(['Real SKU']), how = 'left', left_on = 'SKU', right_on = 'Real SKU')

    temp = data_forstok[data_forstok['Real SKU'].isnull()].copy()
    temp['SKU'] = temp['SKU'].astype(str).str.replace('(S)','', regex = False)
    temp = temp.merge(data_SKU[['Real SKU', 'Real Nama Produk']].drop_duplicates(['Real SKU']), how = 'left', left_on = 'SKU', right_on = 'Real SKU').set_index(temp.index)
    temp['Real SKU_x'] = temp['Real SKU_x'].fillna(temp['Real SKU_y'])
    temp['Real Nama Produk_x'] = temp['Real Nama Produk_x'].fillna(temp['Real Nama Produk_y'])
    temp = temp.drop(['Real SKU_y', 'Real Nama Produk_y'], axis = 1)
    temp = temp.rename(columns = {'Real SKU_x' : 'Real SKU', 'Real Nama Produk_x' : 'Real Nama Produk'})

    indeks = data_forstok[data_forstok['Real SKU'].isnull()].index.to_list()
    data_forstok['Real SKU'][indeks] = temp['Real SKU'][indeks]
    data_forstok['Real Nama Produk'][indeks] = temp['Real Nama Produk'][indeks]

    temp = data_forstok[data_forstok['Real SKU'].isnull()].copy()
    temp['SKU'] = temp['SKU'].astype(str).str.replace('hd','', regex = False)
    temp = temp.merge(data_SKU[['Real SKU', 'Real Nama Produk']].drop_duplicates(['Real SKU']), how = 'left', left_on = 'SKU', right_on = 'Real SKU').set_index(temp.index)
    temp['Real SKU_x'] = temp['Real SKU_x'].fillna(temp['Real SKU_y'])
    temp['Real Nama Produk_x'] = temp['Real Nama Produk_x'].fillna(temp['Real Nama Produk_y'])
    temp = temp.drop(['Real SKU_y', 'Real Nama Produk_y'], axis = 1)
    temp = temp.rename(columns = {'Real SKU_x' : 'Real SKU', 'Real Nama Produk_x' : 'Real Nama Produk'})

    indeks = data_forstok[data_forstok['Real SKU'].isnull()].index.to_list()
    data_forstok['Real SKU'][indeks] = temp['Real SKU'][indeks]
    data_forstok['Real Nama Produk'][indeks] = temp['Real Nama Produk'][indeks]

    data_forstok['Real SKU'] = data_forstok['Real SKU'].astype(str)
    data_forstok = data_forstok.merge(data_SKU[['SKU', 'Brand', 'Sub Brand', 'Parent Item', 'Parent SKU']].drop_duplicates(['SKU']), how = 'left', left_on = 'Real SKU', right_on = 'SKU')
    data_forstok = data_forstok.drop(['SKU_y'], axis = 1)
    data_forstok = data_forstok.rename(columns = {'SKU_x':'SKU'})

    print("--- %s seconds ---" % (time.time() - start_time))
    print("Unbundling ====== 6/10")
    # Forstok Unbundling
    list_col = ['SKU'] + data_SKU.columns[data_SKU.columns.get_loc('Produk 1'):data_SKU.columns.get_loc('Harga Organik 7')+1].to_list()
    data_forstok = data_forstok.merge(data_SKU[list_col].drop_duplicates(['SKU']), how = 'left', left_on = 'Real SKU', right_on = 'SKU')
    list_pcs = [x for x in data_forstok.columns if 'PCS' in x]
    for i in list_pcs:
        data_forstok[i] = data_forstok[i] * data_forstok['Quantity']
    data_forstok = data_forstok.drop(['SKU_y'], axis = 1)
    data_forstok = data_forstok.rename(columns = {'SKU_x':'SKU'})

    indeks = data_forstok[data_forstok['Brand'] == 'Bundle'].index.to_list()
    data_forstok['Bundle Flag'] = np.nan
    data_forstok['Bundle Flag'][indeks] = 'Bundle'

    indeks = data_forstok[data_forstok['Brand'] == 'Bundle'][data_forstok[data_forstok['Brand'] == 'Bundle']['SKU'].astype(str).str.contains('(S)', regex = False)].index.to_list()
    data_forstok['SKU Produk 1'][indeks] = '(S)' + data_forstok['SKU Produk 1'][indeks].astype(str)
    data_forstok['SKU Produk 2'][indeks] = '(S)' + data_forstok['SKU Produk 2'][indeks].astype(str)
    data_forstok['SKU Produk 3'][indeks] = '(S)' + data_forstok['SKU Produk 3'][indeks].astype(str)
    data_forstok['SKU Produk 4'][indeks] = '(S)' + data_forstok['SKU Produk 4'][indeks].astype(str)
    data_forstok['SKU Produk 5'][indeks] = '(S)' + data_forstok['SKU Produk 5'][indeks].astype(str)
    data_forstok['SKU Produk 6'][indeks] = '(S)' + data_forstok['SKU Produk 6'][indeks].astype(str)
    data_forstok['SKU Produk 7'][indeks] = '(S)' + data_forstok['SKU Produk 7'][indeks].astype(str)


    print("--- %s seconds ---" % (time.time() - start_time))
    print("Filling Date ====== 7/10")
    data_forstok['Date'] = np.nan
    data_forstok['Month'] = np.nan
    data_forstok['Year'] = np.nan

    for i in range(data_forstok.shape[0]):
        if int(data_forstok['Order Date'][i].strftime('%d')) < 12:
            data_forstok['Date'][i] = pd.to_datetime(data_forstok['Order Date'][i].strftime('%Y-%d-%m %H:%M')).day
            data_forstok['Month'][i] = pd.to_datetime(data_forstok['Order Date'][i].strftime('%Y-%d-%m %H:%M')).month_name()
            data_forstok['Year'][i] = pd.to_datetime(data_forstok['Order Date'][i].strftime('%Y-%d-%m %H:%M')).year
        else :
            data_forstok['Date'][i] = pd.to_datetime(data_forstok['Order Date'][i]).day
            data_forstok['Month'][i] = pd.to_datetime(data_forstok['Order Date'][i]).month_name()
            data_forstok['Year'][i] = pd.to_datetime(data_forstok['Order Date'][i]).year

    quarter = pd.DataFrame([['January', 1], ['February', 1], ['March', 1], ['April', 2], ['May', 2], ['June', 2],
            ['July', 3], ['August', 3], ['September', 3],['October', 4], ['November', 4], ['December', 4]], columns = ['Bulan', 'Quarter'])
    data_forstok = data_forstok.merge(quarter, how = 'left', left_on = 'Month', right_on = 'Bulan')
    data_forstok = data_forstok.drop(['Bulan'], axis = 1)
    data_bulan = pd.DataFrame([{'Bulan' : 'December', 'Number' : 12} ,
            {'Bulan' : 'January' , 'Number': 1},
            {'Bulan' : 'February' , 'Number': 2},
            {'Bulan' : 'March' , 'Number': 3},
            {'Bulan' : 'April' , 'Number': 4},
            {'Bulan' : 'May' , 'Number': 5},
            {'Bulan' : 'June', 'Number': 6},
            {'Bulan' : 'July' , 'Number': 7},
            {'Bulan' : 'August', 'Number' : 8},
            {'Bulan' : 'September', 'Number' : 9},
            {'Bulan' : 'October' , 'Number': 10},
            {'Bulan' : 'November' , 'Number': 11}])
    temp = data_forstok.copy()
    temp['Day'] = temp['Date']
    temp = temp.merge(data_bulan, how = 'left', left_on = 'Month', right_on='Bulan')
    temp= temp.rename(columns = {'Month' : 'Bulan', 'Number' : 'Month'})
    data_forstok['Week'] = pd.to_datetime(temp[['Year', 'Month', 'Day']]).dt.week
    data_forstok['True datetime'] = pd.to_datetime(temp[['Year', 'Month', 'Day']])

    forstok_all = data_forstok
    forstok_all['Total'] = forstok_all['Sub Total']
    forstok_all['Price List NFI'] = np.nan
    forstok_all['Total Net'] = np.nan

    forstok_all = forstok_all.rename(columns={'Channel Order ID' : 'Order #',
                                            'Status' : 'Order Status',
                                            'Order Date' : 'Order date',
                                            'Item Name' :'Product Name',
                                            'Bundle Name' : 'Bundle',
                                            'Shipping Country' : 'Country',
                                            'Shipping Province' : 'Region',
                                            'Shipping City' : 'City',
                                            'Shipping Zip' : 'Zip Code',
                                            'Shipping Address1' : 'Address',
                                            'Shipping Phone' : 'Phone',
                                            'Quantity' : 'Qty. Invoiced',
                                            'Item Price' : 'Regular Price',
                                            'Sub Total' : 'Subtotal'})
    forstok_all['Kecamatan'] = np.nan
    forstok_all['Kelurahan'] = np.nan
    indeks = forstok_all[forstok_all['City'].astype(str).str.contains('/')]['City'].index.to_list()
    if len(indeks)>0:
        forstok_all['Kecamatan'][indeks] = forstok_all['City'][indeks].str.split('/', n = 1,expand = True)[1]
        forstok_all['City'][indeks] = forstok_all['City'][indeks].str.split('/', n = 1,expand = True)[0]

    indeks = forstok_all[forstok_all['Kecamatan'].astype(str).str.contains('-')]['Kecamatan'].index.to_list()
    if len(indeks)>0:
        forstok_all['Kelurahan'][indeks] = forstok_all['Kecamatan'][indeks].str.split('-', n = 1,expand = True)[1]
        forstok_all['Kecamatan'][indeks] = forstok_all['Kecamatan'][indeks].str.split('-', n = 1,expand = True)[0]

    indeks = forstok_all[forstok_all['City'].astype(str).str.contains(',')]['City'].index.to_list()
    if len(indeks)>0:
        forstok_all['Kecamatan'][indeks] = forstok_all['City'][indeks].str.split(',', n = 1,expand = True)[1]
        forstok_all['City'][indeks] = forstok_all['City'][indeks].str.split(',', n = 1,expand = True)[0]

    indeks = forstok_all[forstok_all['Kecamatan'].astype(str).str.contains(',')]['Kecamatan'].index.to_list()
    if len(indeks)>0:
        forstok_all['Kelurahan'][indeks] = forstok_all['Kecamatan'][indeks].str.split(',', n = 1,expand = True)[1]
        forstok_all['Kecamatan'][indeks] = forstok_all['Kecamatan'][indeks].str.split(',', n = 1,expand = True)[0]

    province = pd.read_excel(r'All Data/list_province.xlsx')
    forstok_all['Region'] = forstok_all['Region'].astype(str)
    province['All Province'] = province['All Province'].astype(str)
    forstok_all['Region'] = forstok_all.merge(province, how = 'left', left_on = 'Region', right_on = 'All Province')['Real Province']

    city = pd.read_excel(r'All Data/list_city.xlsx')
    forstok_all['City'] = forstok_all['City'].astype(str)
    city['All City'] = city['All City'].astype(str)
    forstok_all['City'] = forstok_all.merge(city, how = 'left', left_on = 'City', right_on = 'All City')['Real City']

    district = pd.read_excel(r'All Data/list_district.xlsx')
    forstok_all['Kecamatan'] = forstok_all['Kecamatan'].astype(str)
    district['All District'] = district['All District'].astype(str)
    forstok_all['Kecamatan'] = forstok_all.merge(district, how = 'left', left_on = 'Kecamatan', right_on = 'All District')['Real District']

    data_SKU['Real SKU'] = data_SKU['SKU'].astype(str)
    data_SKU['Real Nama Produk'] = data_SKU['Nama Produk'].astype(str)

    data_bundle1 = forstok_all[~forstok_all['Produk 1'].isnull()]
    data_bundle1['Bundle Name'] = data_bundle1['Product Name']
    data_bundle1['Product Name'] = data_bundle1['Produk 1']
    data_bundle1['SKU'] = data_bundle1['SKU Produk 1']
    data_bundle1['Qty. Invoiced'] = data_bundle1['PCS Produk 1']
    data_bundle1['Price List NFI'] = data_bundle1['Price List NFI 1']
    data_bundle1['Total Net'] = data_bundle1['Price List NFI 1'] * data_bundle1['Qty. Invoiced']
    data_bundle1['Bundle Flag'] = np.nan

    data_bundle2 = forstok_all[~forstok_all['Produk 2'].isnull()]
    data_bundle2['Bundle Name'] = data_bundle2['Product Name']
    data_bundle2['Product Name'] = data_bundle2['Produk 2']
    data_bundle2['SKU'] = data_bundle2['SKU Produk 2']
    data_bundle2['Qty. Invoiced'] = data_bundle2['PCS Produk 2']
    data_bundle2['Price List NFI'] = data_bundle2['Price List NFI 2']
    data_bundle2['Total Net'] = data_bundle2['Price List NFI 2'] * data_bundle2['Qty. Invoiced']
    data_bundle2['Bundle Flag'] = np.nan

    data_bundle3 = forstok_all[~forstok_all['Produk 3'].isnull()]
    data_bundle3['Bundle Name'] = data_bundle3['Product Name']
    data_bundle3['Product Name'] = data_bundle3['Produk 3']
    data_bundle3['SKU'] = data_bundle3['SKU Produk 3']
    data_bundle3['Qty. Invoiced'] = data_bundle3['PCS Produk 3']
    data_bundle3['Price List NFI'] = data_bundle3['Price List NFI 3']
    data_bundle3['Total Net'] = data_bundle3['Price List NFI 3'] * data_bundle3['Qty. Invoiced']
    data_bundle3['Bundle Flag'] = np.nan

    data_bundle4 = forstok_all[~forstok_all['Produk 4'].isnull()]
    data_bundle4['Bundle Name'] = data_bundle4['Product Name']
    data_bundle4['Product Name'] = data_bundle4['Produk 4']
    data_bundle4['SKU'] = data_bundle4['SKU Produk 4']
    data_bundle4['Qty. Invoiced'] = data_bundle4['PCS Produk 4']
    data_bundle4['Price List NFI'] = data_bundle4['Price List NFI 4']
    data_bundle4['Total Net'] = data_bundle4['Price List NFI 4'] * data_bundle4['Qty. Invoiced']
    data_bundle4['Bundle Flag'] = np.nan

    data_bundle5 = forstok_all[~forstok_all['Produk 5'].isnull()]
    data_bundle5['Bundle Name'] = data_bundle5['Product Name']
    data_bundle5['Product Name'] = data_bundle5['Produk 5']
    data_bundle5['SKU'] = data_bundle5['SKU Produk 5']
    data_bundle5['Qty. Invoiced'] = data_bundle5['PCS Produk 5']
    data_bundle5['Price List NFI'] = data_bundle5['Price List NFI 5']
    data_bundle5['Total Net'] = data_bundle5['Price List NFI 5'] * data_bundle5['Qty. Invoiced']
    data_bundle5['Bundle Flag'] = np.nan

    data_bundle6 = forstok_all[~forstok_all['Produk 6'].isnull()]
    data_bundle6['Bundle Name'] = data_bundle6['Product Name']
    data_bundle6['Product Name'] = data_bundle6['Produk 6']
    data_bundle6['SKU'] = data_bundle6['SKU Produk 6']
    data_bundle6['Qty. Invoiced'] = data_bundle6['PCS Produk 6']
    data_bundle6['Price List NFI'] = data_bundle6['Price List NFI 6']
    data_bundle6['Total Net'] = data_bundle6['Price List NFI 6'] * data_bundle6['Qty. Invoiced']
    data_bundle6['Bundle Flag'] = np.nan

    data_bundle7 = forstok_all[~forstok_all['Produk 7'].isnull()]
    data_bundle7['Bundle Name'] = data_bundle7['Product Name']
    data_bundle7['Product Name'] = data_bundle7['Produk 7']
    data_bundle7['SKU'] = data_bundle7['SKU Produk 7']
    data_bundle7['Qty. Invoiced'] = data_bundle7['PCS Produk 7']
    data_bundle7['Price List NFI'] = data_bundle7['Price List NFI 7']
    data_bundle7['Total Net'] = data_bundle7['Price List NFI 7'] * data_bundle7['Qty. Invoiced']
    data_bundle7['Bundle Flag'] = np.nan

    data_bundle = data_bundle1.append([data_bundle2, data_bundle3, data_bundle4, data_bundle5, data_bundle6, data_bundle7], ignore_index = True, sort = False)
    data_bundle['SKU'] = data_bundle['SKU'].astype(str)
    data_bundle['SKU'] = data_bundle['SKU'].str.replace('\.0$', '', regex = True)
    data_bundle[['Real SKU', 'Real Nama Produk', 'Brand', 'Sub Brand', 'Parent Item', 'Parent SKU']] = data_bundle.merge(data_SKU[['Real SKU', 'Nama Produk', 'Brand', 'Sub Brand', 'Parent Item', 'Parent SKU']].drop_duplicates(['Real SKU']), how = 'left', left_on = 'SKU', right_on = 'Real SKU')[['Real SKU_y', 'Nama Produk', 'Brand_y', 'Sub Brand_y', 'Parent Item_y', 'Parent SKU_y']]

    temp = data_bundle[data_bundle['Real SKU'].isnull()].copy()
    temp['SKU'] = temp['SKU'].astype(str).str.replace('(S)','', regex = False)
    temp = temp.merge(data_SKU[['Real SKU', 'Nama Produk', 'Brand', 'Sub Brand', 'Parent Item', 'Parent SKU']].drop_duplicates(['Real SKU']), how = 'left', left_on = 'SKU', right_on = 'Real SKU').set_index(temp.index)

    indeks = data_bundle[data_bundle['Real SKU'].isnull()].index.to_list()
    data_bundle['Real SKU'][indeks] = temp['Real SKU_y'][indeks]
    data_bundle['Real Nama Produk'][indeks] = temp['Nama Produk'][indeks]
    data_bundle['Brand'][indeks] = temp['Brand_y'][indeks]
    data_bundle['Sub Brand'][indeks] = temp['Sub Brand_y'][indeks]
    data_bundle['Parent Item'][indeks] = temp['Parent Item_y'][indeks]
    data_bundle['Parent SKU'][indeks] = temp['Parent SKU_y'][indeks]

    forstok_all = forstok_all.append(data_bundle, ignore_index = True, sort = False)

    print("--- %s seconds ---" % (time.time() - start_time))
    print("Read Masterdata ====== 8/10")
    mylist = []
    data_all = pd.read_csv('Clean Data\data_all.csv', low_memory = False, sep=';', chunksize=20000)
    for chunk in data_all:
        cols = chunk.select_dtypes(include=[np.float64]).columns
        chunk[cols] = chunk[cols].astype(np.float32)
        mylist.append(chunk)
    data_all = pd.concat(mylist, axis= 0)
    del mylist

    # data_all.loc[:, data_all.dtypes == 'float64'] = data_all.loc[:, data_all.dtypes == 'float64'].astype('float32')
    # data_all.loc[:, data_all.dtypes == 'int64'] = data_all.loc[:, data_all.dtypes == 'int64'].astype('int32')


    # data_all = pd.read_csv(r'Clean Data\data_all.csv', index_col = False, sep = ';', low_memory = False)
    data_all = data_all[~data_all['Order #'].astype(str).isin(forstok_all['Order #'].astype(str))]
    cols = forstok_all.select_dtypes(include=[np.float64]).columns
    forstok_all[cols] = forstok_all[cols].astype(np.float32)
    data_all = data_all.append(forstok_all, ignore_index = True, sort = False)
    data_all = data_all.reset_index(drop = True)

    temp = data_all[data_all['Price List NFI'].isnull()].copy()
    temp['Real SKU'] = temp['Real SKU'].astype(str)
    temp = temp.merge(data_SKU[['SKU', 'Price List NFI']].drop_duplicates(['SKU']), how = 'left', left_on = 'Real SKU', right_on = 'SKU').set_index(temp.index)
    temp['Price List NFI_x'] = temp['Price List NFI_y']
    temp = temp.drop(['SKU_y', 'Price List NFI_y'], axis = 1)
    temp = temp.rename(columns = {'SKU_x' : 'SKU', 'Price List NFI_x' : 'Price List NFI'})
    indeks = data_all[data_all['Price List NFI'].isnull()].index.to_list()

    data_all['Price List NFI'][indeks] = temp['Price List NFI'][indeks]
    data_all['Total Net'] = data_all['Price List NFI'] * data_all['Qty. Invoiced']

    data_all['Order #'] = data_all['Order #'].astype(str)
    print("--- %s seconds ---" % (time.time() - start_time))
    print("Export to Masterdata ====== 10/10")
    # data_all = data_all[['Order #', ]]
    # to_csv = data_all.to_csv(r'Clean Data\data_all.csv', index = False, sep = ';')
    print("Export to Masterdata Done")
    if os.path.isfile('ALERT_FORSTOK_SKU_MISSING.xlsx') :
        os.remove('ALERT_FORSTOK_SKU_MISSING.xlsx')

    print("Appending to old data ===== Just leave the program running")
    forstok_old = pd.read_excel(r'All Data\data_forstok_2019.xlsx')
    forstok_old = forstok_old[forstok_old['Sales Order ID'].astype(str).isin(data_forstok_pure['Sales Order ID'].astype(str))]
    forstok_old = forstok_old.append(data_forstok_pure, ignore_index = True, sort = False)
    to_excel = forstok_old.to_excel(r'All Data\data_forstok_2019.xlsx', index = False)
    print("--- %s seconds ---" % (time.time() - start_time))
