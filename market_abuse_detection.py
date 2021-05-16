import pandas as pd
import pandas_datareader.data as pdr
def get_stock_data(filename, stock, start_date, end_date,force_download):
    if force_download:
        # Grabing yahoo finance data and setting as a dataframe
        stock_data = pdr.DataReader(stock, 'yahoo', start_date, end_date)
        # Save the stock data in form of .csv to local
        stock_data.to_csv(filename)

    # force_download is false, then read data from local
    stock_data = pd.read_csv(filename)

    try:
        stock_data.index = pd.to_datetime(stock_data.index, format='%Y-%m-%d')
    except TypeError:
        stock_data.index = pd.to_datetime(stock_data.index)

    return stock_data

def read_df(filename):
    df=pd.read_csv(filename, encoding='latin')
    return df
def str_to_time(filename,col_name):
    filename[col_name]=pd.to_datetime(filename[col_name])
    filename[col_name] = filename[col_name].apply(lambda x: pd.to_datetime(x.date()))
    filename[col_name] = pd.to_datetime(filename[col_name])
    return filename[col_name]

# Define function to filter dataset
def filter_trader_data(df, company, start_date, end_date):
    return df[(df['stockSymbol'] == company) &
              (df['tradeDate'] >= start_date) &
              (df['tradeDatetime'] <= end_date)].reset_index(drop=True)
# Logic to find fraud trading
def find_fraud_trader(df):
    # By default make all rows as fraud trading, then apply logic and update the label.
    df['suspicious'] = True
    df['suspicious'] = df.apply(lambda row: True
    if (pd.isnull(row['High']) | (row['price'] > row['High']) | (row['price'] < row['Low']))
    else False, axis=1)
    return df


# collect data for Amazon from 2020-02-01 to 2020-03-31
stock_data = get_stock_data(filename='stock_data.csv',stock = "AMZN",start_date = '2020-02-01',end_date = '2020-03-31',force_download=True)
stock_data['Date']=str_to_time(stock_data,col_name="Date")

#read trader data
traders_data = read_df(filename='traders_data.csv')
traders_data['tradeDatetime'] = pd.to_datetime(traders_data['tradeDatetime'])
traders_data['tradeDate'] = traders_data['tradeDatetime'].apply(lambda x: pd.to_datetime(x.date()))
traders_data['tradeDate'] = pd.to_datetime(traders_data['tradeDate'])
traders_data = filter_trader_data(traders_data,'AMZN','2020-02-01','2020-03-31')
# Missing value treatment,Deleting the missing values
traders_data.dropna(axis=0, inplace=True)
# Reset the index of traders dataset
traders_data.reset_index(drop=True)
print(traders_data.shape)
# Merger stock & traders datasets
merge_data = pd.merge(traders_data, stock_data, how='left', left_on='tradeDate', right_on='Date',left_index=True, right_index=False)
merge_data = find_fraud_trader(merge_data)


print("Number of trading : ",merge_data.shape[0])
print("Number of suspicious trading : ",merge_data[merge_data['suspicious']==True].shape[0])
print("Number of genuine trading : ",merge_data[merge_data['suspicious']==False].shape[0])

# Rearrange and select necessary columns
data_label = merge_data[['tradeDate','countryCode', 'firstName', 'lastName', 'traderId', 'stockSymbol',
        'tradeId', 'price', 'volume', 'High', 'Low', 'Open', 'Close', 'Adj Close','suspicious']]

print(data_label['suspicious'].value_counts())
print('\n')
print(data_label['suspicious'].value_counts(normalize=True))

# Filter to only fraud data
fraud_data = data_label[data_label['suspicious']==True]
print(fraud_data['traderId'].value_counts())

# Finding details of each fraud trader
freq_fraud_data = fraud_data[['traderId','firstName', 'lastName','countryCode']]
freq_fraud_data.drop_duplicates(inplace=True)
freq_fraud_data.reset_index(drop=True)

# Finding frequency of each fraud trader
traderFreq = fraud_data.groupby(['traderId'])['traderId'].count()
traderFreq = pd.DataFrame({'traderId':traderFreq.index, 'traderFreq':traderFreq.values}).reset_index(drop=True)

# Merge two dataframes
freq_fraud_data = pd.merge(freq_fraud_data,traderFreq,how='inner',on='traderId')
print(freq_fraud_data.sort_values(by='traderFreq', ascending=False))

# Top Fraud Countries
freq_fraud_data['countryFreq'] = freq_fraud_data.groupby('countryCode')['countryCode'].transform('count')


# Trader rank by number of suspicious orders
freq_fraud_data['traderRank'] = freq_fraud_data['traderFreq'].rank(ascending=False)
freq_fraud_data.sort_values(by='traderRank')

# Correlation of nationality with fraud traders
print(freq_fraud_data[['traderFreq','countryFreq']].corr())