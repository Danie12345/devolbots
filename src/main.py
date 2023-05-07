# import required modules
from devolframe import Devolframe as dv

if __name__ == '__main__':
    devol_frame = dv(csv='metricbeabt-so-28042023.csv', block=25e6)

    # uso de nuestro devolframe
    print (devol_frame.genCols())
    # print (devol_frame.findMissing('system', lambda k, v: type(k[v]) == 'object', targetProp='host', n = 8))
    # print (devol_frame.dv.head(10)['@timestamp'].apply(type))
    # print (devol_frame.dv.head(10))