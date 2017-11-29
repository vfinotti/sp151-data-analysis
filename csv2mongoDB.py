
# coding: utf-8

# # Importing data

# In[1]:


import pandas as pd
import numpy as np

dataframe0 = pd.read_csv('sp15620151-semestre.csv', sep=';', encoding='latin1')
dataframe1 = pd.read_csv('sp15620152-semestre.csv', sep=';', encoding='latin1')
dataframe2 = pd.read_csv('sp15620161-semestre.csv', sep=';', encoding='latin1')
dataframe3 = pd.read_csv('sp15620162-semestre.csv', sep=';', encoding='latin1')
dataframe4 = pd.read_csv('sp15620171-semestre.csv', sep=';', encoding='latin1')
dataframe5 = pd.read_csv('sp15620172-semestre.csv', sep=';', encoding='latin1')


# In[2]:


from IPython.core.display import display
with pd.option_context('display.max_rows', 200, 'display.max_columns', 15, 
                       'display.float_format', '{:.2f}'.format):
    display(dataframe0)


# # Pre-procesing data

# In[3]:


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


# In[4]:


dataframe0_fix = fix_subcity(dataframe0)
dataframe1_fix = fix_subcity(dataframe1)
dataframe2_fix = fix_subcity(dataframe2)
dataframe3_fix = fix_subcity(dataframe3)
dataframe4_fix = fix_subcity(dataframe4)
dataframe5_fix = fix_subcity(dataframe5)


# In[5]:


display(dataframe0_fix)


# # Saving data to Mongo DB

# In[31]:


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


# In[34]:


result = df2mongo(dataframe0_fix,
                  dataframe1_fix,
                  dataframe2_fix,
                  dataframe3_fix,
                  dataframe4_fix,
                  dataframe5_fix)


# In[35]:


len(result.inserted_ids)

