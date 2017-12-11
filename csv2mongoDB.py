
# coding: utf-8

# # Importing data

# In[38]:


import pandas as pd
import numpy as np

dataframe0 = pd.read_csv('sp15620151-semestre.csv', sep=';', encoding='latin1', low_memory=False)
dataframe1 = pd.read_csv('sp15620152-semestre.csv', sep=';', encoding='latin1', low_memory=False)
dataframe2 = pd.read_csv('sp15620161-semestre.csv', sep=';', encoding='latin1', low_memory=False)
dataframe3 = pd.read_csv('sp15620162-semestre.csv', sep=';', encoding='latin1', low_memory=False)
dataframe4 = pd.read_csv('sp15620171-semestre.csv', sep=';', encoding='latin1', low_memory=False)
dataframe5 = pd.read_csv('sp15620172-semestre.csv', sep=';', encoding='latin1', low_memory=False)


# In[39]:


from IPython.core.display import display
with pd.option_context('display.max_rows', 50, 'display.max_columns', 15, 
                       'display.float_format', '{:.2f}'.format):
    display(dataframe0)


# In[40]:


print(dataframe0.columns.values)
print(dataframe1.columns.values)
print(dataframe2.columns.values)
print(dataframe3.columns.values)
print(dataframe4.columns.values)
print(dataframe5.columns.values)
print(type(dataframe0.iloc[0,11]))


# # Pre-procesing data

# In[41]:


import sys

def fix_subcity(df):
    df_val = df.values
    df_val_res = df_val
    for idx_row, row in enumerate(df_val):
        if (str(row[11]) !=  'nan') and (str(row[6]) == 'nan'): # info on column 11
            df_val_res[idx_row, 6] = row[11]       
        elif (str(row[11]) != 'nan') and (str(row[6]) != 'nan'): # info on both columns
            if (str(row[11]) != str(row[6])): # different names
                print('Row 11: ', row[11])
                print('Row 6: ', row[6])
                print('Row index: ', idx_row)
                sys.exit("Two sub-city values.")
    return pd.DataFrame(df_val_res[:,0:11], columns = df.columns.values[0:11])


# In[42]:


dataframe0_fix = fix_subcity(dataframe0)


# In[43]:


display(dataframe0_fix)


# In[44]:


dataframe0_fix['Data Abertura'] = pd.to_datetime(dataframe0_fix['Data Abertura'], format='%d/%m/%Y', infer_datetime_format = True)

dataframe1['Data Abertura'] = pd.to_datetime(dataframe1['Data Abertura'], format='%d/%m/%Y')
dataframe1['Data Parecer'] = pd.to_datetime(dataframe1['Data Parecer'], format='%d/%m/%Y')

dataframe2['Data Abertura'] = pd.to_datetime(dataframe2['Data Abertura'], format='%d/%m/%Y')
dataframe2['Data Parecer'] = pd.to_datetime(dataframe2['Data Parecer'], format='%d/%m/%Y')

dataframe3['Data Abertura'] = pd.to_datetime(dataframe3['Data Abertura'], format='%d/%m/%Y')
dataframe3['Data Parecer'] = pd.to_datetime(dataframe3['Data Parecer'], format='%d/%m/%Y')

dataframe4['Data Abertura'] = pd.to_datetime(dataframe4['Data Abertura'], format='%d/%m/%Y')
dataframe4['Data Parecer'] = pd.to_datetime(dataframe4['Data Parecer'], format='%d/%m/%Y')

dataframe5['Data Abertura'] = pd.to_datetime(dataframe5['Data Abertura'], format='%d/%m/%Y')
dataframe5['Data Parecer'] = pd.to_datetime(dataframe5['Data Parecer'], format='%d/%m/%Y')


# # Saving data to Mongo DB

# In[ ]:


from pymongo import MongoClient

def df2mongo(*args):
    # concatenate all inputs in one file
    print('Preparing...')
    lst = []
    for i in args:
        lst.append(i)   
    df_concat = pd.concat(lst)
    
    # dataframe to tuple list
    df_concat_lst = [dict([(colname, row[i]) for i, colname in enumerate(df_concat.columns)]) for row in df_concat.values]
    
    print('Writing to MongoDB...')
    
    # connect to mongoDB and save
    cli = MongoClient()
    database = cli['db_sp151']
    database.sp151.drop()
    result = database.sp151.insert_many(df_concat_lst)
    print('Done!')
    return result


# In[ ]:


result = df2mongo(dataframe0_fix,
                  dataframe1,
                  dataframe2,
                  dataframe3,
                  dataframe4,
                  dataframe5)


# In[ ]:


len(result.inserted_ids)


# # Saving data to SQLite

# Including 'Canal Atendimento' column to < 2017 datasets to assure SQL compatibility

# In[ ]:


dataframe0_fix['Canal Atendimento'] = float('nan')
dataframe1['Canal Atendimento'] = float('nan')
dataframe2['Canal Atendimento'] = float('nan')
dataframe3['Canal Atendimento'] = float('nan')


# In[ ]:


import sqlite3

conn = sqlite3.connect("sp156.db")

dataframe0_fix.to_sql("sp156", conn, if_exists="replace")
dataframe1.to_sql("sp156", conn, if_exists="append")
dataframe2.to_sql("sp156", conn, if_exists="append")
dataframe3.to_sql("sp156", conn, if_exists="append")
dataframe4.to_sql("sp156", conn, if_exists="append")
dataframe5.to_sql("sp156", conn, if_exists="append")

