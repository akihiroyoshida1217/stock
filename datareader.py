import pandas_datareader.data as web
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
from tqdm import tqdm
import pandas as pd
import codecs

data = {}
index_filtered = pd.DataFrame( columns = ['Volume', 'Close', 'Change-1', 'Change-1(%)', 'Change-5', 'Change-5(%)', 'Deviation'])
error_symbols = []

symbols = get_nasdaq_symbols()

for s in tqdm(symbols.index):
    try:
        data[s] = web.DataReader(s,"yahoo","2020/7/1")
        data[s].to_csv("./data/" + s + ".csv")
        if data[s]['Close'][-1] > data[s]['Close'][-6] > data[s]['Close'][-21] and data[s]['Volume'][-1] > 1000000 and ((data[s]['Close'] - data[s]['Close'].rolling(window=25).mean()) / data[s]['Close'].rolling(window=25).mean() * 100)[-1] >= 1:
            index_filtered.loc[s] = [ data[s]['Volume'][-1], data[s]['Close'][-1], data[s]['Close'][-1] - data[s]['Close'][-2], (data[s]['Close'][-1] - data[s]['Close'][-2]) / data[s]['Close'][-2] * 100, data[s]['Close'][-1] - data[s]['Close'][-6], (data[s]['Close'][-1] - data[s]['Close'][-6]) / data[s]['Close'][-6] * 100, ((data[s]['Close'] - data[s]['Close'].rolling(window=25).mean()) / data[s]['Close'].rolling(window=25).mean() * 100)[-1] ]
    except:
        error_symbols.append(s)
print(data, file=codecs.open('data.txt', 'w', 'utf-8'))


#data = [ web.DataReader(s,"yahoo","2020/7/1") for s in tqdm(symbols.index) ]

#data["GOOGL"] = web.DataReader("GOOGL","yahoo","2020/7/1")
#data["FAB"] = web.DataReader("FAB","yahoo","2020/7/1")
##print(data)
#print(data, file=codecs.open('test2.txt', 'w', 'utf-8'))

#print(((data["GOOGL"]['Close'] - data["GOOGL"]['Close'].rolling(window=25).mean()) / data["GOOGL"]['Close'].rolling(window=25).mean() * 100)[-1])
#index_filtered = [ i for i, d in data.items() if d['Close'][-1] > d['Close'][-6] > d['Close'][-21]
#                                                and ((d['Close'] - d['Close'].rolling(window=25).mean()) / d['Close'].rolling(window=25).mean() * 100)[-1] >= 1 ]
#[ data[fi].to_csv(fi + ".csv") for fi in index_filtered ]

index_filtered_sorted = index_filtered.sort_values('Close')
index_filtered_sorted.to_csv("index.csv")
print(index_filtered_sorted)

#data = web.DataReader("GOOGL","yahoo","2020/7/1")
#data.to_csv("sample.csv")
#print(data)
#
#print(data['Close'][-1])
#print(data['Close'][-6])
#print(data['Close'][-21])

