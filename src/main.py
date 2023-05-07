# import required modules
from devolframe import Devolframe as dv
import matplotlib.pyplot as plt

if __name__ == '__main__':
    devol_frame = dv(csv='metricbeabt-so-28042023.csv', block=25e6)

    # uso de nuestro devolframe
    # devol_frame.dv.dropna()
    
    devol_frame.genCols()
    devol_frame.dv.fillna(...)
    # print (devol_frame.dv.head(1101)[['@timestamp', 'cores']].loc[900:'@timestamp'])#.dv.head(200)[['@timestamp', 'cores', 'idle', 'user', 'sys']])
    # print (devol_frame.dv.compute())
    print (devol_frame.dv[['user', 'idle']].corr().compute())
    # print (devol_frame.dv.loc[4.0:10.5].compute())
    # user = devol_frame.dv['user']
    
    # plt.hist(idle, bins=5)
    # plt.show()
    
    # devol_frame.hvplot.scatter(idle='bill_length_mm', user='bill_depth_mm', by='timestamp')
    
    
    # print (devol_frame.findMissing('system', lambda k, v: type(k[v]) == 'object', targetProp='host', n = 8))
    # print (devol_frame.dv.head(10)['@timestamp'].apply(type))
    # print (devol_frame.dv.head(10))