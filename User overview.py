# %%
import pandas as pd
import numpy as np

# %% [markdown]
# Understanding the data. To do this one needs to clean the data.

# %%
import warnings
warnings.filterwarnings('ignore')
pd.set_option('max_column', None)
db = pd.read_csv('data\Week1_challenge_data_source(CSV).csv', na_values=['?', None])
db.head() #returns the first 5 rows.
#pd.set_option('display.float_format',) #review decimal places function

# %% [markdown]
# Data exploration. Find out the content of the data. The table below shows the description of fields in the data.

# %%
#list column names
db.columns.tolist()


# %%
# number of data points
print(f" There are {db.shape[0]} rows and {db.shape[1]} columns")

# %% [markdown]
# Handling missing values.

# %%
# how many missing values exist or better still what is the % of missing values in the dataset?
def percent_missing(df):

    # Calculate total number of cells in dataframe
    totalCells = np.product(df.shape)

    # Count number of missing values per column
    missingCount = df.isnull().sum()

    # Calculate total number of missing values
    totalMissing = missingCount.sum()

    # Calculate percentage of missing values
    print("TellCo's financial dataset contains", round(((totalMissing/totalCells) * 100), 2), "%", "missing values.")

percent_missing(db)

# %%
# Now which column(s) has missing values
db.isna().sum()
#print (db)

# %% [markdown]
# The data above shows the columns and the respective number of missing  values. Since the dataset has 150001 rows, we can drop the rows with the missing values, and still remain with a substantial data for our analysis.

# %%
#drop rows with entirely missing values.
df= db
df.dropna()


# %%
df_clean = db.drop(['Nb of sec with 6250B < Vol UL < 37500B', 'Nb of sec with 37500B < Vol UL', 'Nb of sec with 31250B < Vol DL < 125000B', 'Nb of sec with 125000B < Vol DL','Nb of sec with 1250B < Vol UL < 6250B'], axis=1)
df_clean.shape

# %%
# fill missing with ffill method for columns (diag_1, diag_2, diag_3)

def fix_missing_ffill(df, col):
    df[col] = df[col].fillna(method='ffill')
    return df[col]


def fix_missing_bfill(df, col):
    df[col] = df[col].fillna(method='bfill')
    return df[col]

df_clean['HTTP DL (Bytes)'] = fix_missing_ffill(df_clean, 'HTTP DL (Bytes)')
df_clean['HTTP UL (Bytes)'] = fix_missing_ffill(df_clean, 'HTTP UL (Bytes)')
df_clean['Avg RTT DL (ms)'] = fix_missing_ffill(df_clean, 'Avg RTT DL (ms)')
df_clean['Avg RTT DL (ms)'] = fix_missing_ffill(df_clean, 'Avg RTT DL (ms)')
df_clean['TCP DL Retrans. Vol (Bytes)'] = fix_missing_ffill(df_clean, 'TCP DL Retrans. Vol (Bytes)')
df_clean['TCP UL Retrans. Vol (Bytes)'] = fix_missing_ffill(df_clean, 'TCP UL Retrans. Vol (Bytes)')
df_clean['Handset Type'] = df_clean['Handset Type'].fillna(df_clean['Handset Type'].mode()[0])

percent_missing(df_clean)


# %% [markdown]
# TellCo's financial dataset has reduced from 12% to 1.72%
# 

# %% [markdown]
# Data Exploration and Filtering. We are trying to extract data fro the database. Here, we are filtering the top ten handset used by customers.

# %%
all = df_clean['Handset Type'].value_counts().head(10)
print(all)
all.plot(kind="pie", title="The top 10 handset used by customers")

# %% [markdown]
# The top three handset manufacturers 

# %%
mode= df_clean['Handset Manufacturer'].mode()
df_clean['Handset Manufacturer'].fillna(mode,inplace=True)
x = df_clean['Handset Manufacturer'].value_counts().head(3)
print(x)
x.plot(kind="pie", title="The top 3 Handset manufacturers");

# %% [markdown]
# The top 3 handset manufacturers are; Apple, Samsung and Huawei. We are now going to list all the top handset types in the three manufacturers.

# %%
#top 5 Huawei handset types
top3 = df_clean.groupby('Handset Manufacturer')['Handset Type'].value_counts()['Huawei'].head(5)
print(top3)
top3.plot(kind="pie" , title=" top 5 Huawei handset")

# %%
#top 5 Apple handset types.
apple = df_clean.groupby('Handset Manufacturer')['Handset Type'].value_counts()['Apple'].head()
print(apple)
apple.plot(kind="pie",  title = 'top 5 Apple handset')

# %%
#top 5 Samsung handset types.
samsung = df_clean.groupby('Handset Manufacturer')['Handset Type'].value_counts()['Samsung'].head()
print(samsung)
samsung.plot(kind="pie", title = 'top 5 Samsung handset')

# %%
#Check the frequency of user sessions. The results should show a total duration for each user in ms.
sessionsCountData=df_clean['MSISDN/Number'].value_counts().head()

sessionsCount=sessionsCountData.values.tolist()

msisdn=sessionsCountData.index.values

sessionPerUserDictionary = dict(zip(msisdn, sessionsCount))

print(sessionPerUserDictionary)
df_clean.groupby('MSISDN/Number')['Dur. (ms)'].sum()


# %%
#Calculationg total DL and UL per user, filtered to the first 20.
df_clean = df_clean.rename(columns = {'Total DL (Bytes)' : 'totalDL','Total UL (Bytes)' : 'totalUL'})
sum_column = df_clean["totalUL"] + df_clean["totalDL"]
df_clean["totalData"] = sum_column
totalDataDF=df_clean.groupby('MSISDN/Number')['totalData'].sum()
totalDataValues = totalDataDF.values
msisdn=totalDataDF.index.values

dataPerUser = dict(zip(msisdn, totalDataValues))

dict_items = dataPerUser.items()
first_twenty = list(dict_items)[:20]
first_twenty

# %%
#The above results show the aggregate data used by each customer on many different sessions
df_clean=df_clean.rename(columns = {'Total DL (Bytes)' : 'totalDL','Total UL (Bytes)' : 'totalUL','Dur. (ms)' : 'dur','MSISDN/Number':'msisdn','Last Location Name':'location','Handset Manufacturer':'manufacturer','Handset Type':'handset'})

sum_column = df_clean["totalUL"] + df_clean["totalDL"]


google = df_clean['Google DL (Bytes)']+ df_clean['Google UL (Bytes)']
email = df_clean['Email DL (Bytes)']+ df_clean['Email UL (Bytes)']
gaming = df_clean['Gaming DL (Bytes)']+ df_clean['Gaming UL (Bytes)']
youtube = df_clean['Youtube DL (Bytes)']+ df_clean['Youtube UL (Bytes)']
netflix = df_clean['Netflix DL (Bytes)']+ df_clean['Netflix UL (Bytes)']
social = df_clean['Social Media DL (Bytes)']+ df_clean['Social Media UL (Bytes)']

df_clean['google']=google
df_clean['email']=email
df_clean['gaming']=gaming
df_clean['youtube']=youtube
df_clean['netflix']=netflix
df_clean['social']=social

relevant_data=df_clean[['msisdn', 'google','email','gaming','youtube','netflix','social']]
relevant_data["totalData"] = sum_column
relevant_data.groupby('msisdn')['totalData'].sum()
msisdn



