import pandas_datareader.data as web
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
#from tqdm import tqdm
import pandas as pd
import codecs
from multiprocessing import Pool
import multiprocessing

def read_data(symbol):
    #data = {}
    error_symbols = []
    
    try:
        data = web.DataReader(symbol,"yahoo","2020/7/1")
        data.to_csv("./data/" + symbol + ".csv")
        return data
    except:
        error_symbols.append(symbol)

def search_data(data):
    
    try:
        deviation = (data['Close'] - data['Close'].rolling(window=25).mean()) / data['Close'].rolling(window=25).mean() * 100
        return data['Volume'][-1] > 1000000 \
            and data['Close'][-1] > data['Close'][-6] > data['Close'][-21] \
            and deviation[-1] >= 3 \
            and (deviation[-21:-1] >= -1).sum() == 20
    except:
        return False

def get_data(item):
    error_symbols = []
    symbol = item[0] 
    data = item[1] 
    
    try:
        deviation = (data['Close'] - data['Close'].rolling(window=25).mean()) / data['Close'].rolling(window=25).mean() * 100
        data_extracted = [ symbol,
            data['Volume'][-1], 
            data['Close'][-1], data['Close'][-1] - data['Close'][-2], 
            (data['Close'][-1] - data['Close'][-2]) / data['Close'][-2] * 100, 
            data['Close'][-1] - data['Close'][-6], 
            (data['Close'][-1] - data['Close'][-6]) / data['Close'][-6] * 100, 
            deviation[-1] ]
        return data_extracted
    except:
        error_symbols.append(symbol)

if __name__ == '__main__':

    symbols = get_nasdaq_symbols()

    with Pool(multiprocessing.cpu_count()) as pool_for_read:
        index_dataframe = {i: d for i, d in zip(symbols.index, pool_for_read.map(read_data, symbols.index))}

    with Pool(multiprocessing.cpu_count()) as pool_for_search:
        index_dataframe_filtered = {i: d for i, d, result in zip(list(index_dataframe.keys()), list(index_dataframe.values()), pool_for_search.map(search_data, list(index_dataframe.values()))) if result}

    with Pool(multiprocessing.cpu_count()) as pool_for_get:
        index_data_extracted = pool_for_get.map(get_data, list(index_dataframe_filtered.items()))

    index_dataframe_extracted = pd.DataFrame( index_data_extracted, columns = ['symbol', 'Volume', 'Close', 'Change-1', 'Change-1(%)', 'Change-5', 'Change-5(%)', 'Deviation']).set_index('symbol')
    index_dataframe_sorted = index_dataframe_extracted.sort_values('Close')
    index_dataframe_sorted.to_csv("index.csv")
    print(index_dataframe_sorted)

    #index_filtered_list = list(map(symbol_search, symbols.index))
    #print(index_filtered_list, file=codecs.open('index_filtered_list.txt', 'w', 'utf-8'))

    #index_filtered = pd.DataFrame( index_filtered_list, columns = ['symbol', 'Volume', 'Close', 'Change-1', 'Change-1(%)', 'Change-5', 'Change-5(%)', 'Deviation']).set_index('symbol')

    #index_filtered_sorted = index_filtered.sort_values('Close')
    #index_filtered_sorted.to_csv("index.csv")
    #print(index_filtered_sorted)

