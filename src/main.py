# import required modules
import json
import pandas as pd
import numpy as np
import time
from dask import dataframe as df1
from devolframe import Devolframe as dv


if __name__ == '__main__':
    print (1 if None else 0)
    # time taken to read data
    start_time = time.time()
    data_frame = dv('metricbeabt-so-28042023.csv', block=25e6)
    end_time = time.time()

    print("Read with dask: ", (end_time-start_time), "seconds")

    # data
    print (data_frame.findMissing('system', lambda k, v: type(k[v]) == 'object', targetProp='host', n = 8))
    # print (data_frame.dv.head(10)['@timestamp'].apply(type))
    # print (data_frame.dv.head(10))

    ######## PRUEBAS SIN DASK XD
    # limit = 100
    # with open('metricbeabt-so-28042023.csv') as file:
    # file = dask_df
    # for line in file.readlines():
    #     # x = line.split("{'cpu':")
    #     a,b,c,d,*e = line.split(',')
    #     e = ''.join(e)[1:-2].replace('} \'', '}, \'')
    #     e = json.loads(f"\"{e}\"")
    #     try:
    #         print (a)
    #         print (e)
    #     except: pass
        
    #     limit -= 1
    #     if limit < 0:
    #         break
    # {'cpu': {'cores': 4, 'idle': {'pct': 3.8109}, 'user': {'pct': 0.0813}, 'system': {'pct': 0.1078}}}